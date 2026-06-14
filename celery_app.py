"""
celery_app.py — الـ Celery Instance المشترك

يُستورَد من كل من web.py و worker.py لضمان نفس الإعداد.
لا تضع هنا أي كود Flask لتجنب الـ circular imports.
"""
from celery import Celery
import config

celery = Celery(
    "online_app",
    broker=config.CELERY_BROKER_URL,
    backend=config.CELERY_RESULT_BACKEND,
    include=["tasks.job_tasks"],  # الـ modules التي تحتوي المهام
)

celery.conf.update(
    task_serializer=config.CELERY_TASK_SERIALIZER,
    result_serializer=config.CELERY_RESULT_SERIALIZER,
    accept_content=config.CELERY_ACCEPT_CONTENT,
    timezone=config.CELERY_TIMEZONE,
    enable_utc=config.CELERY_ENABLE_UTC,

    # ─── إعدادات الأداء ───────────────────────────────────────────────────
    # الـ worker يُنجز مهمة واحدة ثم يطلب أخرى (يمنع تراكم المهام في الذاكرة)
    worker_prefetch_multiplier=1,
    task_acks_late=True,             # يؤكد إنجاز المهمة بعد التنفيذ وليس قبله

    # ─── Result Expiry ────────────────────────────────────────────────────
    result_expires=3600,             # نتائج المهام تُحذف بعد ساعة من Redis

    # ─── Celery Beat Schedule (يستبدل _watcher_loop) ─────────────────────
    # يُشغّل مهمة مسح المهام المجدولة كل دقيقة
    beat_schedule={
        "scan-due-jobs-every-minute": {
            "task":     "tasks.job_tasks.scan_and_dispatch_due_jobs",
            "schedule": 60.0,  # كل 60 ثانية
        },
    },
)
