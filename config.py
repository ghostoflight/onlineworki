"""
config.py — الإعدادات المركزية للمشروع
كل القيم تُقرأ من Environment Variables لضمان الأمان
"""
import os
import sys

# ─── Database (Supabase / PostgreSQL) ───────────────────────────────────────
# في Supabase: Settings → Database → Connection String → "Transaction pooler"
# استخدم رابط الـ pooler (port 6543) وليس الـ direct (5432) في Railway
# NOTE: do NOT hard-crash on a missing var — keep the process alive so the
# /debug/health endpoint can report exactly what's missing (no logs needed).
DATABASE_URL: str = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("[config] FATAL: DATABASE_URL is not set in this service's env!", file=sys.stderr)

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

# ─── Telegram Bot (بوت مشترك تفاعلي) ─────────────────────────────────────────
TELEGRAM_BOT_TOKEN      = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_BOT_USERNAME   = os.environ.get("TELEGRAM_BOT_USERNAME", "")   # بدون @ (لروابط الربط العميقة)
TELEGRAM_WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "") # سرّ عشوائي يحمي مسار الـ webhook

# العنوان العام: يُؤخذ من PUBLIC_BASE_URL أو يُشتق تلقائياً من Railway
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "") or (
    f"https://{os.environ['RAILWAY_PUBLIC_DOMAIN']}"
    if os.environ.get("RAILWAY_PUBLIC_DOMAIN") else ""
)


# ─── Diagnostics: force-log exactly what THIS process resolved ───────────────
def _mask(url: str) -> str:
    """Hide credentials in a URL for safe logging."""
    if not url:
        return "<EMPTY>"
    import re
    return re.sub(r"://[^@/]+@", "://***:***@", url)


def summary() -> dict:
    """A safe, masked snapshot of the resolved config (for /debug/health)."""
    return {
        "DATABASE_URL":          _mask(DATABASE_URL),
        "DATABASE_URL_set":      bool(os.environ.get("DATABASE_URL")),
        "CELERY_BROKER_URL":     _mask(CELERY_BROKER_URL),
        "CELERY_RESULT_BACKEND": _mask(CELERY_RESULT_BACKEND),
        "REDIS_URL_set":         bool(os.environ.get("REDIS_URL")),
        "PUBLIC_BASE_URL":       PUBLIC_BASE_URL or "<EMPTY>",
        "TELEGRAM_BOT_TOKEN_set":      bool(TELEGRAM_BOT_TOKEN),
        "TELEGRAM_WEBHOOK_SECRET_set": bool(TELEGRAM_WEBHOOK_SECRET),
        "DEBUG":                 DEBUG,
    }


def log_summary() -> None:
    """Print the resolved config to stderr at startup (visible in Railway logs)."""
    print("[config] ===== resolved configuration this process sees =====", file=sys.stderr)
    for k, v in summary().items():
        print(f"[config]   {k} = {v}", file=sys.stderr)
    print("[config] ========================================================", file=sys.stderr)
