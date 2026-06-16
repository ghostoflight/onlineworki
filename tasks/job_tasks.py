"""
tasks/job_tasks.py — مهام Celery

الـ Worker يُشغّل هذا الكود معزولاً عن الـ Flask API تماماً.

تحديث هذه النسخة (عزل البيانات):
  • يُقرأ os مباشرةً من سجل المهمة (scheduled_jobs.os) لا من tg_env العام.
  • توافق رجعي: المهام القديمة بلا os تُعامَل كأندرويد (advertising_id).
"""
import json
import logging
from datetime import datetime, timezone
from urllib.parse import quote

import requests

from celery_app import celery
import config
from db.connection import get_conn

logger = logging.getLogger(__name__)


def _build_proxies(host: str, port: str, user: str, passwd: str) -> dict | None:
    if not host:
        return None
    creds = ""
    if user:
        creds = f"{quote(str(user), safe='')}:{quote(str(passwd or ''), safe='')}@"
    p   = port or "80"
    url = f"http://{creds}{host}:{p}"
    return {"http": url, "https": url}


def _send_telegram(token: str, chat_id: str, text: str) -> None:
    """إرسال إشعار Telegram — الأخطاء لا توقف المهمة الرئيسية."""
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=8,
        )
    except Exception as e:
        logger.warning(f"[Telegram] Failed to send notification: {e}")


def _get_user_env(user_id) -> dict:
    """يقرأ بيئة المختبِر (tg_env) من user_data: afid / gaid / idfa (للتوافق الرجعي)."""
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


def _notify_enabled(user_id) -> bool:
    """مُفعّل افتراضياً ما لم يُضبط notify_enabled صراحةً على '0'."""
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


@celery.task(name="tasks.job_tasks.scan_and_dispatch_due_jobs")
def scan_and_dispatch_due_jobs() -> dict:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    dispatched = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE scheduled_jobs
                SET enabled = 0
                WHERE enabled = 1
                  AND run_at IS NOT NULL
                  AND run_at <= NOW()
                RETURNING id, name
            """)
            due_jobs = cur.fetchall()

    for job in due_jobs:
        jid = job["id"]
        logger.info(f"[Beat] Dispatching job {jid}: {job['name']}")
        execute_job.apply_async(args=[jid], countdown=0)
        dispatched.append(jid)

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

    # ════════════════════════════════════════════════════════════════════
    # ① State-Locked — تُقرأ بيانات البيئة مرة واحدة وتُجمَّد.
    # عزل البيانات: os من سجل المهمة مباشرةً (لا من tg_env العام).
    # afid/device_id يبقيان بمنطقهما الحالي (env ثم سجل المهمة) للتوافق الرجعي.
    # ════════════════════════════════════════════════════════════════════
    env      = _get_user_env(job["user_id"])          # لـ afid/device fallback فقط
    os_      = (job.get("os") or "").lower().strip()  # ← من سجل المهمة (معزول)
    if not os_:
        os_ = "android"                               # توافق رجعي: مهام قديمة بلا os → أندرويد
    afid     = (env.get("afid") or job.get("afid") or "").strip()
    dev_key  = (job.get("dev_key") or "").strip()

    if os_ == "ios":
        device_id = (env.get("idfa") or job.get("gaid") or "").strip()
    else:
        device_id = (env.get("gaid") or job.get("gaid") or "").strip()

    events  = job["events"] if isinstance(job["events"], list) else json.loads(job["events"] or "[]")
    proxies = _build_proxies(
        job.get("proxy_host", ""), job.get("proxy_port", ""),
        job.get("proxy_user", ""), job.get("proxy_pass", ""),
    )

    # ════════════════════════════════════════════════════════════════════
    # ② Fail-Fast Validation Gate (os صار له افتراضي آمن، فلا يُفحَص هنا)
    # ════════════════════════════════════════════════════════════════════
    def _critical_abort(field: str) -> dict:
        error_msg = f"CRITICAL: Missing {field}"
        logger.error(f"[Worker] Job {job_id} aborted — {error_msg}")
        _update_job_status(job_id, "failed", error_msg)
        return {"job_id": job_id, "status": "failed", "reason": error_msg}

    if not dev_key:
        return _critical_abort("dev_key")

    if not device_id:
        field = "idfa" if os_ == "ios" else "advertising_id (gaid)"
        return _critical_abort(field)

    output_log = ""
    all_ok     = True

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
            # المفتاح الصحيح بحسب os الخاص بالمهمة (عزل البيانات)
            if os_ == "ios":
                body["idfa"] = device_id
            else:
                body["advertising_id"] = device_id

            resp = requests.post(
                f"https://api2.appsflyer.com/inappevent/{job['package']}",
                headers={"authentication": dev_key},
                json=body,
                proxies=proxies,
                timeout=15,
            )
            ok_event = resp.status_code in (200, 201)
            output_log += f"[{ev_name}] → {resp.status_code}\n"
            if not ok_event:
                all_ok = False
            else:
                sent_events.add(ev_name)

            _log_event_history(job["user_id"], job["package"], ev_name, resp.status_code, ok_event)

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

    try:
        if user and user.get("tg_chat_id") and _notify_enabled(job["user_id"]):
            token = config.TELEGRAM_BOT_TOKEN or user.get("tg_token") or ""
            if token:
                icon  = "✅" if all_ok else "⚠️"
                short = output_log.replace("\n", " | ")[:150]
                msg   = (
                    f"{icon} نتيجة الاختبار\n"
                    f"المهمة: {job['name']}\n"
                    f"الحالة: {final_status}\n"
                    f"السجلّ: {short}"
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
