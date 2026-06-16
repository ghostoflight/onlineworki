"""
db/connection.py — طبقة الاتصال بـ PostgreSQL (Supabase)

نستخدم psycopg2 مع ConnectionPool لتجنب استنفاد الـ connections
في Railway عند تشغيل workers متعددين.

إصلاح هذه النسخة: تهيئة الـ pool آمنة ضد التزامن (double-checked locking)
حتى لا يُنشَأ أكثر من pool عند أول طلبين متزامنين.
"""
import contextlib
import logging
import threading
from typing import Generator

import psycopg2
from psycopg2 import pool, extras

import config

logger = logging.getLogger(__name__)

# ─── Connection Pool ─────────────────────────────────────────────────────────
_pool: pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def get_pool() -> pool.ThreadedConnectionPool:
    """يُنشئ الـ pool مرة واحدة فقط ويعيده (Singleton آمن ضد التزامن)."""
    global _pool
    if _pool is None or _pool.closed:
        with _pool_lock:
            if _pool is None or _pool.closed:
                _pool = pool.ThreadedConnectionPool(
                    minconn=2,
                    maxconn=60,
                    dsn=config.DATABASE_URL,
                    cursor_factory=extras.RealDictCursor,
                )
                logger.info("[DB] Connection pool initialized.")
    return _pool


@contextlib.contextmanager
def get_conn() -> Generator:
    """Context manager يُعيد connection من الـ pool ويُعيده تلقائياً عند الانتهاء."""
    p = get_pool()
    conn = p.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        p.putconn(conn)


def init_db() -> None:
    """يُنشئ الجداول إن لم تكن موجودة. يُستدعى مرة واحدة عند بدء تشغيل الـ web service."""
    schema = """
        CREATE TABLE IF NOT EXISTS users (
            id         SERIAL PRIMARY KEY,
            username   TEXT UNIQUE NOT NULL,
            password   TEXT NOT NULL,
            role       TEXT DEFAULT 'user',
            max_uses   INTEGER DEFAULT 100,
            uses_left  INTEGER DEFAULT 100,
            expire_at  TIMESTAMPTZ DEFAULT NULL,
            created    TIMESTAMPTZ DEFAULT NOW(),
            active     INTEGER DEFAULT 1,
            tg_token   TEXT DEFAULT NULL,
            tg_chat_id TEXT DEFAULT NULL
        );

        CREATE TABLE IF NOT EXISTS sessions (
            token      TEXT PRIMARY KEY,
            user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created    TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS user_data (
            id         SERIAL PRIMARY KEY,
            user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            key        TEXT NOT NULL,
            value      TEXT,
            updated    TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, key)
        );

        CREATE TABLE IF NOT EXISTS scheduled_jobs (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            name        TEXT NOT NULL,
            events      JSONB NOT NULL DEFAULT '[]',
            proxy_host  TEXT DEFAULT '',
            proxy_port  TEXT DEFAULT '',
            proxy_user  TEXT DEFAULT '',
            proxy_pass  TEXT DEFAULT '',
            package     TEXT DEFAULT '',
            dev_key     TEXT DEFAULT '',
            gaid        TEXT DEFAULT '',
            afid        TEXT DEFAULT '',
            os          TEXT DEFAULT '',
            run_at      TIMESTAMPTZ,
            enabled     INTEGER DEFAULT 1,
            last_run    TIMESTAMPTZ,
            last_status TEXT,
            last_output TEXT,
            created     TIMESTAMPTZ DEFAULT NOW()
        );

        -- توافق رجعي: إضافة العمود للجداول الموجودة مسبقاً دون فقدان بيانات
        ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS os TEXT DEFAULT '';

        CREATE TABLE IF NOT EXISTS job_logs (
            id      SERIAL PRIMARY KEY,
            job_id  INTEGER NOT NULL REFERENCES scheduled_jobs(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL DEFAULT 0,
            ran_at  TIMESTAMPTZ DEFAULT NOW(),
            status  TEXT,
            output  TEXT
        );

        CREATE TABLE IF NOT EXISTS event_history (
            id         SERIAL PRIMARY KEY,
            user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            game       TEXT,
            event_name TEXT,
            status     INTEGER,
            ok         INTEGER DEFAULT 0,
            type       TEXT DEFAULT 'sent',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_user    ON sessions(user_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_user        ON scheduled_jobs(user_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_run_at      ON scheduled_jobs(run_at) WHERE enabled = 1;
        CREATE INDEX IF NOT EXISTS idx_history_user     ON event_history(user_id);
        CREATE INDEX IF NOT EXISTS idx_job_logs_job     ON job_logs(job_id);
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(schema)

            import hashlib
            admin_pw = hashlib.sha256(b"admin123").hexdigest()
            cur.execute("""
                INSERT INTO users (username, password, role, max_uses, uses_left)
                VALUES ('admin', %s, 'admin', 999999, 999999)
                ON CONFLICT (username) DO NOTHING
            """, (admin_pw,))

    logger.info("[DB] Schema ready.")
