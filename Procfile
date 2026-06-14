# Procfile — تعريف الخدمات في Railway
#
# كيفية الإعداد في Railway:
# 1. أضف هذا الملف لجذر المشروع
# 2. في Railway Dashboard → افتح المشروع → أضف 3 Services من نفس الـ repo
# 3. في إعدادات كل Service، غيّر الـ "Start Command" للأمر المقابل أدناه

# ── Service 1: web (Flask API) ─────────────────────────────────────────────
web: gunicorn web:app --workers=2 --worker-class=gthread --threads=4 --bind=0.0.0.0:$PORT --timeout=60 --log-level=info

# ── Service 2: worker (Celery Task Executor) ──────────────────────────────
worker: celery -A worker.celery worker --loglevel=info --concurrency=4 --max-tasks-per-child=100

# ── Service 3: beat (Celery Scheduler — يستبدل _watcher_loop) ────────────
beat: celery -A worker.celery beat --loglevel=info --scheduler=celery.beat:PersistentScheduler
