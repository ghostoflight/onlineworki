"""
worker.py — Celery worker / Beat entrypoint (no Flask here).

Its only job: import the configured celery instance and register the tasks.
Run on Railway:
    celery -A worker.celery worker --loglevel=info --concurrency=4
With Beat (scheduler):
    celery -A worker.celery beat --loglevel=info
"""
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# Pre-configured celery instance (broker/result backend live in celery_app.py)
from celery_app import celery  # noqa: F401 — required by the Celery CLI

# Import tasks so they self-register on the worker
import tasks.job_tasks  # noqa: F401

# ── Worker-side reliability tuning for a Redis broker ────────────────────────
# These are safe to set here (worker/beat process only); the web process keeps
# celery_app.py's config. acks_late + prefetch=1 = fair dispatch and re-queue
# on crash instead of silently losing a task.
celery.conf.task_acks_late = True
celery.conf.worker_prefetch_multiplier = 1
celery.conf.broker_connection_retry_on_startup = True
# Redis visibility timeout > our longest task+retry window, so a slow job isn't
# redelivered to a second worker while the first is still running it.
celery.conf.broker_transport_options = {
    **(celery.conf.broker_transport_options or {}),
    "visibility_timeout": 3600,
}

# ── Ensure the periodic scan is scheduled (idempotent / non-destructive) ─────
# The scan is lock-free: a single atomic UPDATE ... RETURNING claims due jobs,
# so overlapping beat ticks can't double-dispatch. setdefault won't override an
# entry already defined in celery_app.py.
_beat = dict(celery.conf.beat_schedule or {})
_beat.setdefault("scan-due-jobs", {
    "task": "tasks.job_tasks.scan_and_dispatch_due_jobs",
    "schedule": 60.0,
})
celery.conf.beat_schedule = _beat
