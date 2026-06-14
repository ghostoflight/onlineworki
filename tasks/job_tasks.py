"""
tasks/job_tasks.py — مهام Celery

هذا الملف يحتوي على المنطق الفعلي للتنفيذ في الخلفية.
الـ Worker يُشغّل هذا الكود معزولاً عن الـ Flask API تماماً.

إصلاحات هذه النسخة:
  • المطالبة الذرّية بالمهام المستحقة (يمنع الإرسال المزدوج عند تداخل دورات Beat).
  • معالجة ConnectionError على مستوى الحدث بدل إعادة تشغيل المهمة كاملةً
    (يمنع تكرار إرسال الأحداث التي نجحت — الأحداث ليست idempotent).
  • Exponential backoff حقيقي عند إعادة المحاولة قبل أي إرسال.
  • ترميز بيانات اعتماد البروكسي (يمنع كسر الرابط بمحارف خاصة).
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


# ─── Helper: بناء إعدادات الـ Proxy ──────────────────────────────────────────
def _build_proxies(host: str, port: str, user: str, passwd: str) -> dict | None:
    if not host:
        return None
    creds = ""
    if user:
        # ترميز آمن للمستخدم/كلمة المرور (قد تحتوي @ : / ...)
        creds = f"{quote(str(user), safe='')}:{quote(str(passwd or ''), safe='')}@"
    p   = port or "80"
    url = f"http://{creds}{host}:{p}"
    return {"http": url, "https": url}


# ─── Helper: إرسال إشعار Telegram ────────────────────────────────────────────
def _send_telegram(token: str, chat_id: str, text: str) -> None:
    """إرسال إشعار Telegram — الأخطاء لا توقف المهمة الرئيسية."""
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=8,
        )
    except Exception as e:
        logger.warning(f"[Telegram] Failed to send notification: {e}")


# ─── المهمة المجدولة: مسح Jobs المستحقة كل دقيقة ────────────────────────────
@celery.task(name="tasks.job_tasks.scan_and_dispatch_due_jobs")
def scan_and_dispatch_due_jobs() -> dict:
    """
    تُستدعى كل دقيقة من Celery Beat.

    تستخدم *مطالبة ذرّية*: عبارة UPDATE ... RETURNING واحدة تُعطّل المهام
    المستحقة وتستعيدها معاً، فلا يمكن لدورتَي Beat متتاليتين التقاط نفس
    المهمة وإرسالها مرتين (إصلاح سباق الإرسال المزدوج).
    """
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


# ─── المهمة الرئيسية: تنفيذ Job واحد ────────────────────────────────────────
@celery.task(
    name="tasks.job_tasks.execute_job",
    bind=True,                          # self يُتيح إعادة المحاولة
    max_retries=config.TASK_MAX_RETRIES,
    default_retry_delay=config.TASK_RETRY_BACKOFF,
    acks_late=True,
)
def execute_job(self, job_id: int) -> dict:
    """
    تنفّذ جميع الـ Events الخاصة بـ Job معين وتحفظ نتائجها.

    سياسة إعادة المحاولة (آمنة ضد التكرار):
        - تُعيد تشغيل المهمة كاملةً *فقط* إذا حدث خطأ اتصال قبل إرسال أي حدث.
        - بعد إرسال أي حدث، أخطاء الاتصال تُسجَّل كفشل للحدث ولا تُعيد التشغيل
          (حتى لا تتكرّر الأحداث التي نجحت مسبقاً).
        - الـ backoff أُسّي: الانتظار يتضاعف مع كل محاولة.
    """
    logger.info(f"[Worker] Starting job {job_id}")

    # ── جلب بيانات المهمة من قاعدة البيانات ──────────────────────────────
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM scheduled_jobs WHERE id = %s", (job_id,))
            job = cur.fetchone()
            if not job:
                logger.error(f"[Worker] Job {job_id} not found.")
                return {"error": "Job not found"}

            cur.execute("SELECT * FROM users WHERE id = %s", (job["user_id"],))
            user = cur.fetchone()

    # ── تجهيز المتغيرات ───────────────────────────────────────────────────
    events  = job["events"] if isinstance(job["events"], list) else json.loads(job["events"] or "[]")
    proxies = _build_proxies(
        job.get("proxy_host", ""), job.get("proxy_port", ""),
        job.get("proxy_user", ""), job.get("proxy_pass", ""),
    )

    output_log = ""
    all_ok     = True
    progressed = False   # هل أُرسِل/سُجِّل أي حدث؟ (مفتاح تفادي التكرار عند Retry)

    # ── تنفيذ كل Event بشكل منفرد ────────────────────────────────────────
    for ev in events:
        ev_name = ev.get("name", "").replace("{}", "1")
        try:
            resp = requests.post(
                f"https://api2.appsflyer.com/inappevent/{job['package']}",
                headers={"authentication": job.get("dev_key") or ""},
                json={
                    "appsflyer_id":   job.get("afid") or "",
                    "advertising_id": job.get("gaid") or "",
                    "eventName":      ev_name,
                    "eventTime":      datetime.now(timezone.utc).isoformat(),
                    "eventValue":     "{}",
                },
                proxies=proxies,
                timeout=15,
            )
            ok_event = resp.status_code in (200, 201)
            output_log += f"[{ev_name}] → {resp.status_code}\n"
            if not ok_event:
                all_ok = False
            progressed = True

            _log_event_history(job["user_id"], job["package"], ev_name, resp.status_code, ok_event)

        except requests.Timeout:
            output_log += f"[{ev_name}] → TIMEOUT\n"
            all_ok = False
            progressed = True
            logger.warning(f"[Worker] Job {job_id} event '{ev_name}' timed out.")

        except requests.ConnectionError as exc:
            # إعادة تشغيل المهمة كاملةً مسموحة فقط قبل إرسال أي حدث.
            if not progressed and self.request.retries < self.max_retries:
                backoff = config.TASK_RETRY_BACKOFF * (2 ** self.request.retries)
                logger.warning(
                    f"[Worker] Job {job_id} connection error before any send — "
                    f"retry #{self.request.retries + 1} in {backoff}s ({exc})"
                )
                raise self.retry(exc=exc, countdown=backoff)
            # بعد بدء الإرسال: سجّل الفشل وتابع بقية الأحداث دون تكرار
            output_log += f"[{ev_name}] → CONNECTION_ERROR\n"
            all_ok = False
            progressed = True
            logger.warning(f"[Worker] Job {job_id} event '{ev_name}' connection error: {exc}")

        except Exception as exc:
            output_log += f"[{ev_name}] → ERROR: {str(exc)[:60]}\n"
            all_ok = False
            progressed = True
            logger.error(f"[Worker] Job {job_id} unexpected error: {exc}")

    # ── تحديث حالة المهمة في قاعدة البيانات ──────────────────────────────
    final_status = "success" if all_ok else "partial_error"
    _update_job_status(job_id, final_status, output_log[:2000])

    # ── إرسال إشعار Telegram (إن كان مُفعّلاً) ───────────────────────────
    if user and user.get("tg_token") and user.get("tg_chat_id"):
        icon     = "✅" if all_ok else "⚠️"
        short    = output_log.replace("\n", " | ")[:150]
        msg_text = f"{icon} *Job Done*\nTask: `{job['name']}`\nStatus: `{final_status}`\nLogs: `{short}`"
        _send_telegram(user["tg_token"], user["tg_chat_id"], msg_text)

    logger.info(f"[Worker] Job {job_id} finished → {final_status}")
    return {"job_id": job_id, "status": final_status}


# ─── Helpers داخلية ───────────────────────────────────────────────────────────
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
                        enabled     = 0         -- أوقف المهمة بعد التنفيذ
                    WHERE id = %s
                """, (status, output, job_id))
                cur.execute("""
                    INSERT INTO job_logs (job_id, user_id, status, output)
                    SELECT id, user_id, %s, %s FROM scheduled_jobs WHERE id = %s
                """, (status, output, job_id))
    except Exception as e:
        logger.error(f"[DB] Failed to update job status: {e}")
