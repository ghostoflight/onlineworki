"""
worker.py — نقطة دخول Celery Worker

هذا الملف لا يحتوي على أي كود Flask.
مسؤوليته الوحيدة: استيراد الـ celery instance وتشغيل الـ worker.

التشغيل في Railway:
    celery -A worker.celery worker --loglevel=info --concurrency=4

التشغيل مع Beat (الجدولة):
    celery -A worker.celery beat --loglevel=info
"""
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# استيراد الـ celery instance المُهيّأ مسبقاً
from celery_app import celery  # noqa: F401 — مطلوب لـ Celery CLI

# استيراد المهام لتسجيلها تلقائياً في الـ worker
import tasks.job_tasks  # noqa: F401
