"""
tasks/job_tasks.py — Celery tasks (runs in the worker, isolated from Flask).

Aligned with the refactored controller:
  • os is read from the job record (scheduled_jobs.os), not global tg_env.
  • dev_key is strictly package-specific (from the job row); never silently
    swapped for a global key — a missing key is logged and fails fast.
  • Deep dispatch logging (URL / masked headers / payload) before each POST,
    mirroring telegram_bot._dispatch_event.
  • Telegram result notifications are localized via locales.lookup() using the
    user's stored language — no hardcoded strings.
"""
import json
import logging
from datetime import datetime, timezone
from urllib.parse import quote

import requests

from celery_app import celery
import config
import locales
from db.connection import get_conn

logger = logging.getLogger(__name__)

APPSFLYER_URL = "https://api2.appsflyer.com/inappevent/{package}"


def _mask_key(dev_key: str) -> str:
    return (dev_key[:4] + "…" + str(len(dev_key)) + "c") if dev_key else "<none>"


def _dev_key_for_package(package: str) -> str:
    """
    Resolve the AppsFlyer dev_key for a package from games_config.GAMES_DATA.

    The dev_key is CONFIG-ONLY — it is never asked from the user and never stored
    in the DB job row. We match the job's `package` against the static config and
    return its key. Returns '' if the package is absent or has no key.
    """
    try:
        from games_config import GAMES_DATA
    except Exception as e:
        logger.error("[Worker] cannot import games_config.GAMES_DATA: %s", e)
        return ""
    target = str(package or "").strip()
    for g in GAMES_DATA:
        if str(g.get("package", "")).strip() == target:
            return str(g.get("dev_key", "") or "").strip()
    logger.error("[Worker] package '%s' not found in games_config.GAMES_DATA (%d apps configured).",
                 target, len(GAMES_DATA))
    return ""


def _build_proxies(host: str, port: str, user: str, passwd: str, scheme: str = "http") -> dict | None:
    if not host:
        return None
    scheme = (scheme or "http").lower().strip()
    creds = ""
    if user:
        creds = f"{quote(str(user), safe='')}:{quote(str(passwd or ''), safe='')}@"
    p   = port or "80"
    url = f"{scheme}://{creds}{host}:{p}"
    # SOCKS5 needs the 'requests[socks]' (PySocks) extra installed.
    return {"http": url, "https": url}


def _send_telegram(token: str, chat_id: str, text: str) -> None:
    """Best-effort Telegram notification — failures never stop the task."""
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=8,
        )
    except Exception as e:
        logger.warning(f"[Telegram] Failed to send notification: {e}")


def _get_user_env(user_id) -> dict:
    """Reads tg_env (afid / gaid / idfa) — used only as a backward-compat fallback."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT value FROM user_data WHERE user_id = %s AND key = 'tg_env'",
                    (user_id,),
                )
                row = cur.fetchone()
        return json.loads(row["value"]) if row and row.get("value") else {}
    except Exception as e:
        logger.warning(f"[Worker] read tg_env failed: {e}")
        return {}


def _get_user_lang(user_id) -> str:
    """Reads the user's language from user_data; falls back to the default."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT value FROM user_data WHERE user_id = %s AND key = 'lang'",
                    (user_id,),
                )
                row = cur.fetchone()
        lang = (row["value"] if row and row.get("value") else "") or locales.DEFAULT_LANG
        return lang if lang in locales.SUPPORTED else locales.DEFAULT_LANG
    except Exception as e:
        logger.warning(f"[Worker] read lang failed: {e}")
        return locales.DEFAULT_LANG


def _notify_enabled(user_id) -> bool:
    """Enabled by default unless notify_enabled is explicitly '0'."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT value FROM user_data WHERE user_id = %s AND key = 'notify_enabled'",
                    (user_id,),
                )
                row = cur.fetchone()
        return (row["value"] if row else "1") == "1"
    except Exception as e:
        logger.warning(f"[Worker] read notify_enabled failed: {e}")
        return True


@celery.task(name="tasks.job_tasks.debug_ping")
def debug_ping() -> str:
    """Round-trip probe: proves web → Redis → worker → result backend all work."""
    logger.info("[Ping] debug_ping executed on the worker ✅")
    return "pong"


@celery.task(name="tasks.job_tasks.scan_and_dispatch_due_jobs")
def scan_and_dispatch_due_jobs() -> dict:
    """
    Beat entrypoint (every minute). Lock-free claim: a single
    UPDATE ... RETURNING flips due jobs to enabled=0 and returns them atomically,
    so two overlapping beat ticks can never grab the same job twice, and a
    container restart can never double-dispatch (the row is already claimed).
    """
    now = datetime.now(timezone.utc)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    logger.info("[Beat] scan tick @ %s UTC — checking for due jobs…", now_str)
    dispatched = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE scheduled_jobs
                    SET enabled = 0
                    WHERE enabled = 1
                      AND run_at IS NOT NULL
                      AND run_at <= NOW()
                    RETURNING id, name, run_at
                """)
                due_jobs = cur.fetchall()
    except Exception as e:
        logger.error("[Beat] scan query FAILED: %s", e)
        return {"dispatched": [], "error": str(e), "checked_at": now_str}

    if not due_jobs:
        logger.info("[Beat] no due jobs this tick.")
        return {"dispatched": [], "checked_at": now_str}

    logger.info("[Beat] %d job(s) due — dispatching to the worker queue.", len(due_jobs))
    for job in due_jobs:
        jid = job["id"]
        try:
            execute_job.apply_async(args=[jid], countdown=0)
            logger.info("[Beat] queued job %s (%s), run_at=%s", jid, job["name"], job.get("run_at"))
            dispatched.append(jid)
        except Exception as e:
            # re-enable so the next tick retries (broker hiccup, etc.)
            logger.error("[Beat] failed to queue job %s: %s — re-enabling for retry", jid, e)
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE scheduled_jobs SET enabled = 1 WHERE id = %s", (jid,))
            except Exception:
                pass

    return {"dispatched": dispatched, "checked_at": now_str}


@celery.task(
    name="tasks.job_tasks.execute_job",
    bind=True,
    max_retries=config.TASK_MAX_RETRIES,
    default_retry_delay=config.TASK_RETRY_BACKOFF,
    acks_late=True,
)
def execute_job(self, job_id: int, _sent_events: list | None = None) -> dict:
    logger.info(f"[Worker] Starting job {job_id} (retry={self.request.retries})")

    sent_events: set[str] = set(_sent_events or [])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM scheduled_jobs WHERE id = %s", (job_id,))
            job = cur.fetchone()
            if not job:
                logger.error(f"[Worker] Job {job_id} not found.")
                return {"error": "Job not found"}

            cur.execute("SELECT * FROM users WHERE id = %s", (job["user_id"],))
            user = cur.fetchone()

    # ── State-Locked: read env once, freeze locals. os from the job record. ──
    env      = _get_user_env(job["user_id"])          # afid/device fallback only
    os_      = (job.get("os") or "").lower().strip()
    if not os_:
        os_ = "android"                               # backward-compat default
    afid     = (env.get("afid") or job.get("afid") or "").strip()
    package  = job["package"]
    url      = APPSFLYER_URL.format(package=package)

    # dev_key is NOT stored in the DB and NOT asked from the user — it is resolved
    # at run time from games_config.py by matching the job's package.
    dev_key = str(job.get("dev_key") or _dev_key_for_package(package)).strip()

    if os_ == "ios":
        device_id = (env.get("idfa") or job.get("gaid") or "").strip()
    else:
        device_id = (env.get("gaid") or job.get("gaid") or "").strip()

    events  = job["events"] if isinstance(job["events"], list) else json.loads(job["events"] or "[]")
    proxies = _build_proxies(
        job.get("proxy_host", ""), job.get("proxy_port", ""),
        job.get("proxy_user", ""), job.get("proxy_pass", ""),
        scheme=(job.get("proxy_scheme") or "http"),
    )

    # ── Fail-Fast gate (os has a safe default, so it isn't checked here) ──────
    def _critical_abort(field: str) -> dict:
        error_msg = f"CRITICAL: Missing {field}"
        logger.error(f"[Worker] Job {job_id} aborted — {error_msg}")
        _update_job_status(job_id, "failed", error_msg)
        return {"job_id": job_id, "status": "failed", "reason": error_msg}

    if not dev_key:
        logger.error("[Worker] Job %s: package '%s' is missing from games_config.py "
                     "(or has no dev_key) — cannot resolve key, aborting.", job_id, package)
        return _critical_abort(f"dev_key for package '{package}' in games_config")

    if not device_id:
        field = "idfa" if os_ == "ios" else "advertising_id (gaid)"
        return _critical_abort(field)

    logger.debug(
        "[Worker] job=%s package=%s os=%s device_id=%s afid=%s dev_key=%s events=%d proxied=%s",
        job_id, package, os_, bool(device_id), bool(afid), _mask_key(dev_key), len(events), bool(proxies),
    )

    output_log = ""
    all_ok     = True
    headers    = {"Content-Type": "application/json", "authentication": dev_key}

    for ev in events:
        ev_name = ev.get("name", "").replace("{}", "1")

        if ev_name in sent_events:
            logger.info(f"[Worker] Job {job_id} skipping already-sent event '{ev_name}'")
            output_log += f"[{ev_name}] → SKIPPED (sent in prior retry)\n"
            continue

        try:
            body = {
                "appsflyer_id": afid,
                "eventName":    ev_name,
                "eventTime":    datetime.now(timezone.utc).isoformat(),
                "eventValue":   "{}",
            }
            # correct identifier key per the job's os (data isolation)
            if os_ == "ios":
                body["idfa"] = device_id
            else:
                body["advertising_id"] = device_id

            # ── Deep debug: exact URL / masked headers / payload before POST ──
            logger.debug(
                "[Worker] POST %s | headers={Content-Type:application/json, authentication:%s} | payload=%s",
                url, _mask_key(dev_key), json.dumps(body, ensure_ascii=False),
            )

            resp = requests.post(url, headers=headers, json=body, proxies=proxies, timeout=15)
            ok_event = resp.status_code in (200, 201)
            output_log += f"[{ev_name}] → {resp.status_code}\n"
            if not ok_event:
                all_ok = False
                logger.error(
                    "[Worker] job=%s package=%s event=%s status=%s resp=%s",
                    job_id, package, ev_name, resp.status_code, (resp.text or "")[:200],
                )
            else:
                sent_events.add(ev_name)

            _log_event_history(job["user_id"], package, ev_name, resp.status_code, ok_event)

        except requests.Timeout:
            output_log += f"[{ev_name}] → TIMEOUT\n"
            all_ok = False
            logger.warning(f"[Worker] Job {job_id} event '{ev_name}' timed out.")

        except requests.ConnectionError as exc:
            if not sent_events and self.request.retries < self.max_retries:
                backoff = config.TASK_RETRY_BACKOFF * (2 ** self.request.retries)
                logger.warning(
                    f"[Worker] Job {job_id} connection error before any send — "
                    f"retry #{self.request.retries + 1} in {backoff}s ({exc})"
                )
                raise self.retry(
                    exc=exc,
                    countdown=backoff,
                    kwargs={"_sent_events": list(sent_events)},
                )
            output_log += f"[{ev_name}] → CONNECTION_ERROR\n"
            all_ok = False
            logger.warning(f"[Worker] Job {job_id} event '{ev_name}' connection error: {exc}")

        except Exception as exc:
            output_log += f"[{ev_name}] → ERROR: {str(exc)[:60]}\n"
            all_ok = False
            logger.error(f"[Worker] Job {job_id} unexpected error: {exc}")

    final_status = "success" if all_ok else "partial_error"
    _update_job_status(job_id, final_status, output_log[:2000])

    # ── Localized result notification (isolated; never breaks the worker) ────
    try:
        if user and user.get("tg_chat_id") and _notify_enabled(job["user_id"]):
            token = config.TELEGRAM_BOT_TOKEN or user.get("tg_token") or ""
            if token:
                lang  = _get_user_lang(job["user_id"])
                icon  = "✅" if all_ok else "⚠️"
                short = output_log.replace("\n", " | ")[:150]
                msg   = locales.lookup(
                    "worker_result", lang,
                    icon=icon, name=job["name"], status=final_status, log=short,
                )
                _send_telegram(token, user["tg_chat_id"], msg)
    except Exception as e:
        logger.warning(f"[Telegram] notification skipped (worker continues): {e}")

    logger.info(f"[Worker] Job {job_id} finished → {final_status}")
    return {"job_id": job_id, "status": final_status}


def _log_event_history(user_id, game, event_name, status, ok):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO event_history (user_id, game, event_name, status, ok)
                    VALUES (%s, %s, %s, %s, %s)
                """, (user_id, game, event_name, status, 1 if ok else 0))
    except Exception as e:
        logger.error(f"[DB] Failed to log event history: {e}")


def _update_job_status(job_id, status, output):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE scheduled_jobs
                    SET last_status = %s,
                        last_output = %s,
                        last_run    = NOW(),
                        enabled     = 0
                    WHERE id = %s
                """, (status, output, job_id))
                cur.execute("""
                    INSERT INTO job_logs (job_id, user_id, status, output)
                    SELECT id, user_id, %s, %s FROM scheduled_jobs WHERE id = %s
                """, (status, output, job_id))
    except Exception as e:
        logger.error(f"[DB] Failed to update job status: {e}")
