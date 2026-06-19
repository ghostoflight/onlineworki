"""
celery_app.py — الـ Celery Instance المشترك

يُستورَد من كل من web.py و worker.py لضمان نفس الإعداد.
لا تضع هنا أي كود Flask لتجنب الـ circular imports.

ملاحظة مهمة: إنشاء Celery() لا يفتح اتصالاً بـ Redis (الاتصال كسول/lazy)،
لذا استيراد هذا الملف لا يُسقِط تطبيق الويب حتى لو كان Redis غير متاح.
الاتصال يحدث فقط عند أول apply_async/connection — ونضبط مهلات قصيرة كي يفشل
بسرعة بدل أن يُعلّق خيط gunicorn.
"""
from celery import Celery
import config

celery = Celery(
    "online_app",
    broker=config.CELERY_BROKER_URL,
    backend=config.CELERY_RESULT_BACKEND,
    include=["tasks.job_tasks"],
)

# Short, explicit timeouts so a broker problem fails FAST instead of hanging a
# gunicorn worker thread (a hung thread is what Railway kills → looks like a 500).
_TRANSPORT_OPTS = {
    "socket_timeout": 5,
    "socket_connect_timeout": 5,
    "visibility_timeout": 3600,
}

celery.conf.update(
    task_serializer=config.CELERY_TASK_SERIALIZER,
    result_serializer=config.CELERY_RESULT_SERIALIZER,
    accept_content=config.CELERY_ACCEPT_CONTENT,
    timezone=config.CELERY_TIMEZONE,
    enable_utc=config.CELERY_ENABLE_UTC,

    # ─── Performance ──────────────────────────────────────────────────────
    worker_prefetch_multiplier=1,
    task_acks_late=True,

    # ─── Connection safety (fail fast, retry on boot) ─────────────────────
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=3,
    broker_transport_options=_TRANSPORT_OPTS,
    result_backend_transport_options=_TRANSPORT_OPTS,
    redis_socket_timeout=5,
    redis_socket_connect_timeout=5,
    # don't enter the 20× reconnect loop on a dead backend — fail fast so a
    # web request (or /debug check) can't hang a gunicorn thread for ~20s.
    result_backend_always_retry=False,
    result_backend_max_retries=3,
    # cap publish retries so apply_async() raises promptly when the broker is down
    task_publish_retry_policy={"max_retries": 2, "interval_start": 0,
                               "interval_step": 0.3, "interval_max": 1},

    # ─── Result Expiry ────────────────────────────────────────────────────
    result_expires=3600,

    # ─── Celery Beat Schedule ─────────────────────────────────────────────
    beat_schedule={
        "scan-due-jobs-every-minute": {
            "task":     "tasks.job_tasks.scan_and_dispatch_due_jobs",
            "schedule": 60.0,
        },
    },
)


def broker_ping(timeout: int = 5):
    """
    Connection-safe broker probe. Returns (ok: bool, error: str|None).
    Never raises — safe to call from a request handler / health check.
    """
    try:
        with celery.connection() as conn:
            conn.ensure_connection(max_retries=1, timeout=timeout)
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
