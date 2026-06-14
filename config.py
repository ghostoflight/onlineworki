"""
config.py — الإعدادات المركزية للمشروع
كل القيم تُقرأ من Environment Variables لضمان الأمان
"""
import os

# ─── Database (Supabase / PostgreSQL) ───────────────────────────────────────
# في Supabase: Settings → Database → Connection String → "Transaction pooler"
# استخدم رابط الـ pooler (port 6543) وليس الـ direct (5432) في Railway
DATABASE_URL: str = os.environ["DATABASE_URL"]   # سيرفع استثناءً إن لم يُعيَّن

# ─── Redis (Message Broker + Result Backend) ─────────────────────────────────
# في Railway: أضف Redis plugin، ثم خذ REDIS_URL من المتغيرات
REDIS_URL: str = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# ─── Celery ──────────────────────────────────────────────────────────────────
CELERY_BROKER_URL       = REDIS_URL
CELERY_RESULT_BACKEND   = REDIS_URL

# نُفعّل الـ serializer بصيغة JSON لأن الـ pickle غير آمن
CELERY_TASK_SERIALIZER          = "json"
CELERY_RESULT_SERIALIZER        = "json"
CELERY_ACCEPT_CONTENT           = ["json"]
CELERY_TIMEZONE                 = "UTC"
CELERY_ENABLE_UTC               = True

# ─── Retry Policy (سياسة إعادة المحاولة للمهام) ─────────────────────────────
# عدد مرات إعادة المحاولة عند فشل طلب API خارجي
TASK_MAX_RETRIES   = int(os.environ.get("TASK_MAX_RETRIES", "3"))
# الانتظار بين كل محاولة بالثواني (Exponential Backoff)
TASK_RETRY_BACKOFF = int(os.environ.get("TASK_RETRY_BACKOFF", "60"))

# ─── Flask ───────────────────────────────────────────────────────────────────
SECRET_KEY = os.environ.get("SECRET_KEY", "change-me-in-production")
DEBUG      = os.environ.get("FLASK_DEBUG", "0") == "1"
