"""
worker.py — Celery worker / Beat entrypoint (no Flask here).

Its only job: import the configured celery instance and register the tasks.

Run on Railway (THREE separate services from the same repo):
    web:    gunicorn web:app --workers=2 --worker-class=gthread --threads=4 --bind=0.0.0.0:$PORT
    worker: celery -A worker.celery worker --loglevel=info --concurrency=4 --max-tasks-per-child=100
    beat:   celery -A worker.celery beat   --loglevel=info

If you can only afford ONE extra service, fold Beat into the worker process:
    celery -A worker.celery worker -B --loglevel=info --concurrency=4
(-B = embedded beat; fine for a single worker, not for horizontally-scaled workers.)
"""
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# Make the scheduler's own activity visible in Railway logs (the equivalent of
# raising the APScheduler logger): you will see Beat wake up and send the task.
logging.getLogger("celery.beat").setLevel(logging.INFO)
logging.getLogger("celery.worker").setLevel(logging.INFO)
logging.getLogger("tasks.job_tasks").setLevel(logging.INFO)

# Pre-configured celery instance (broker/result backend + beat_schedule live in
# celery_app.py — both worker and beat import the SAME instance, so the schedule
# is guaranteed present regardless of which process launches).
from celery_app import celery  # noqa: F401 — required by the Celery CLI

# Import tasks so they self-register on the worker
import tasks.job_tasks  # noqa: F401

# Funnel this container's logs into the shared Postgres sink so /debug/logs on
# the web service can show them. Label as "beat" or "worker" based on the CLI.
try:
    from db_logging import install_db_logging
    _svc = "beat" if any("beat" in a for a in sys.argv) else "worker"
    install_db_logging(_svc)
except Exception as _e:
    print(f"[db_logging] install failed: {_e}", file=sys.stderr)

logging.getLogger(__name__).info(
    "[Boot] Celery entry loaded. beat_schedule=%s | broker reachable via CELERY_BROKER_URL",
    list((celery.conf.beat_schedule or {}).keys()),
)

# ── Worker-side reliability tuning for a Redis broker ────────────────────────
celery.conf.task_acks_late = True
celery.conf.worker_prefetch_multiplier = 1
celery.conf.broker_connection_retry_on_startup = True
celery.conf.broker_transport_options = {
    **(celery.conf.broker_transport_options or {}),
    "visibility_timeout": 3600,
}

# ── Ensure the periodic scan is scheduled (only if celery_app didn't already) ─
_beat = dict(celery.conf.beat_schedule or {})
_scan_task = "tasks.job_tasks.scan_and_dispatch_due_jobs"
if not any((e or {}).get("task") == _scan_task for e in _beat.values()):
    _beat["scan-due-jobs"] = {"task": _scan_task, "schedule": 60.0}
    celery.conf.beat_schedule = _beat
