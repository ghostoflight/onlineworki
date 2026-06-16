"""
db/connection.py — PostgreSQL (Supabase) connection layer.

Thread-safe by design: a single ThreadedConnectionPool is shared across the
Flask web workers, the Celery workers, AND the background threads that
telegram_bot.py now spawns for outbound API calls. Every borrow/return goes
through get_conn(), which guarantees the connection is committed/rolled-back
and returned to the pool even on error — no leaks across threads.
"""
import contextlib
import logging
import threading
from typing import Generator

import psycopg2
from psycopg2 import pool, extras

import config

logger = logging.getLogger(__name__)

# minconn=1  : one always-warm connection
# maxconn=12 : ceiling for concurrent borrowers (gunicorn workers × threads +
#              celery concurrency). Keep ≤ your Supabase pooler limit.
_pool: pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def get_pool() -> pool.ThreadedConnectionPool:
    """Create the pool once and return it (thread-safe singleton, double-checked)."""
    global _pool
    if _pool is None or _pool.closed:
        with _pool_lock:
            if _pool is None or _pool.closed:
                _pool = pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=12,
                    dsn=config.DATABASE_URL,
                    cursor_factory=extras.RealDictCursor,
                    # FIX 1 — TCP Keepalives (Silent Drop Fix):
                    # Supabase's PgBouncer forcefully drops idle connections after ~5 min.
                    # These socket-level keepalives probe the connection every 30 s of
                    # inactivity, retrying up to 5 times at 10 s intervals, so the OS
                    # detects and evicts dead sockets before psycopg2 tries to reuse them.
                    # This eliminates the 3-5 s TCP-timeout stall on the first query after
                    # an idle period.
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
                logger.info("[DB] Connection pool initialized.")
    return _pool


@contextlib.contextmanager
def get_conn() -> Generator:
    """
    Borrow a connection from the pool and return it automatically.

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ...")

    Behaviour:
      • commits on clean exit, rolls back on exception (then re-raises);
      • ALWAYS returns the connection to the pool (finally);
      • a connection that died (conn.closed != 0) is discarded instead of being
        handed back to the pool — this prevents a broken socket from poisoning
        another thread/worker that borrows it next.
    """
    p = get_pool()
    conn = p.getconn()                     # raises if pool is exhausted (caller handles)
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass                           # connection already gone; discarded below
        raise
    finally:
        broken = bool(getattr(conn, "closed", 0))
        try:
            p.putconn(conn, close=broken)  # close=True ⇒ drop dead connection
        except Exception as e:
            logger.warning(f"[DB] putconn failed (discarding): {e}")


def close_all() -> None:
    """Close every pooled connection (use on graceful shutdown)."""
    global _pool
    with _pool_lock:
        if _pool is not None and not _pool.closed:
            _pool.closeall()
            logger.info("[DB] Connection pool closed.")


def init_db() -> None:
    """Create tables if absent. Called once at web-service startup."""
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
            proxy_scheme TEXT DEFAULT 'http',
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

        -- backward-compat: add columns on pre-existing tables without data loss
        ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS os TEXT DEFAULT '';
        ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS proxy_scheme TEXT DEFAULT 'http';

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

        -- FIX 2 — Missing Index (Sequential Scan Fix):
        -- Every Telegram update triggers _user_by_chat(chat_id), which queries
        -- users WHERE tg_chat_id = %s. Without this index Postgres does a full
        -- sequential scan of the users table on every single webhook call.
        -- This B-tree index reduces that lookup from O(n) to O(log n).
        CREATE INDEX IF NOT EXISTS idx_users_tg_chat_id ON users(tg_chat_id);
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
