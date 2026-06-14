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
            json={"chat_id": chat_id, "text": text},
            timeout=8,
        )
    except Exception as e:
        logger.warning(f"[Telegram] Failed to send notification: {e}")


# ─── المهمة المجدولة: مسح Jobs المستحقة كل دقيقة ────────────────────────────
def _get_user_env(user_id) -> dict:
    """يقرأ بيئة المختبِر (tg_env) من user_data: os / gaid / idfa / afid / proxy."""
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

    # ── توجيه ديناميكي للبيانات حسب بيئة المختبِر (user_data.tg_env) ──────
    env = _get_user_env(job["user_id"])
    os_ = (env.get("os") or "").lower()
    afid = env.get("afid") or job.get("afid") or ""
    if os_ == "ios":
        device_id = env.get("idfa") or job.get("gaid") or ""
    else:  # android أو غير محدّد → advertising_id
        device_id = env.get("gaid") or job.get("gaid") or ""

    output_log = ""
    all_ok     = True
    progressed = False   # هل أُرسِل/سُجِّل أي حدث؟ (مفتاح تفادي التكرار عند Retry)

    # ── تنفيذ كل Event بشكل منفرد ────────────────────────────────────────
    for ev in events:
        ev_name = ev.get("name", "").replace("{}", "1")
        try:
            body = {
                "appsflyer_id": afid,
                "eventName":    ev_name,
                "eventTime":    datetime.now(timezone.utc).isoformat(),
                "eventValue":   "{}",
            }
            # توجيه المعرّف: iOS → idfa، Android (أو غير محدّد) → advertising_id
            if os_ == "ios":
                body["idfa"] = device_id
            else:
                body["advertising_id"] = device_id

            resp = requests.post(
                f"https://api2.appsflyer.com/inappevent/{job['package']}",
                headers={"authentication": job.get("dev_key") or ""},
                json=body,
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

    # ── إشعار Telegram — مشروط بـ notify_enabled ومعزول تماماً ───────────
    # try/except منفصل: أي خطأ في تلغرام يجب ألا يوقف أو يؤثّر على الـ Worker.
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
