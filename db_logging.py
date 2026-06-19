"""
db_logging.py — shared Postgres log sink for ALL containers (web, worker, beat).

Design goals:
  • NON-BLOCKING: log calls only push a tuple onto an in-process queue (O(1),
    drops instead of blocking if the queue is full). A single daemon thread
    drains the queue and batch-inserts into `system_logs`.
  • NO POOL CONTENTION / NO FEEDBACK LOOPS: the sink thread uses its OWN
    dedicated psycopg2 connection (never db.connection.get_conn), and never
    calls logging itself (errors go to stderr only). This prevents a logging
    write from consuming an app pool slot or recursively generating more logs.
  • SELF-PROVISIONING: creates the table on first connect (idempotent), and
    periodically prunes old rows so the table can't grow unbounded.

Usage (call once per process, at startup):
    from db_logging import install_db_logging
    install_db_logging("web")      # or "worker" / "beat"
"""
import logging
import os
import queue
import sys
import threading
import time

import psycopg2

import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS system_logs (
    id         BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    service    TEXT,
    level      TEXT,
    logger     TEXT,
    message    TEXT
);
CREATE INDEX IF NOT EXISTS idx_system_logs_id ON system_logs (id DESC);
"""

_MAX_ROWS       = 5000      # keep roughly the last 5000 rows
_FLUSH_INTERVAL = 1.0       # seconds between flushes
_BATCH          = 100       # or flush early once this many are buffered
_QUEUE_MAX      = 10000     # hard cap; excess log records are dropped (never block)

_installed = False
_install_lock = threading.Lock()


class _DBSinkThread(threading.Thread):
    """Drains the queue and batch-inserts rows using a dedicated connection."""

    def __init__(self, q: "queue.Queue"):
        super().__init__(daemon=True, name="db-log-sink")
        self.q = q
        self._conn = None
        self._flushes = 0

    def _connect(self):
        if self._conn is None or getattr(self._conn, "closed", 1):
            self._conn = psycopg2.connect(config.DATABASE_URL)
            self._conn.autocommit = True
            with self._conn.cursor() as cur:
                cur.execute(SCHEMA)
        return self._conn

    def _write(self, rows):
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO system_logs (service, level, logger, message) "
                    "VALUES (%s,%s,%s,%s)",
                    rows,
                )
                self._flushes += 1
                if self._flushes % 20 == 0:        # occasional retention prune
                    cur.execute(
                        "DELETE FROM system_logs WHERE id < "
                        "(SELECT COALESCE(MAX(id), 0) - %s FROM system_logs)",
                        (_MAX_ROWS,),
                    )
        except Exception as e:
            # NEVER use logging here — it would feed back into the queue. stderr only.
            self._conn = None
            print(f"[db_logging] sink write failed (dropping {len(rows)} rows): {e}",
                  file=sys.stderr)

    def run(self):
        buf, last = [], time.time()
        while True:
            timeout = max(0.05, _FLUSH_INTERVAL - (time.time() - last))
            try:
                buf.append(self.q.get(timeout=timeout))
            except queue.Empty:
                pass
            if buf and (len(buf) >= _BATCH or (time.time() - last) >= _FLUSH_INTERVAL):
                self._write(buf)
                buf, last = [], time.time()


class _QueuePutHandler(logging.Handler):
    """Formats a record into a row tuple and enqueues it (non-blocking)."""

    def __init__(self, q: "queue.Queue", service: str):
        super().__init__()
        self.q = q
        self.service = service

    def emit(self, record):
        try:
            row = (self.service, record.levelname, record.name, self.format(record))
            self.q.put_nowait(row)
        except queue.Full:
            pass            # drop rather than block the caller
        except Exception:
            pass


def install_db_logging(service: str = None, level=logging.INFO) -> bool:
    """
    Attach the non-blocking Postgres sink to the root logger. Idempotent.
    `service` labels the origin container ("web"/"worker"/"beat").
    """
    global _installed
    with _install_lock:
        if _installed:
            return True
        if not config.DATABASE_URL:
            print("[db_logging] DATABASE_URL not set — sink disabled", file=sys.stderr)
            return False
        service = service or os.environ.get("SERVICE_NAME") or "app"

        q = queue.Queue(maxsize=_QUEUE_MAX)
        handler = _QueuePutHandler(q, service)
        handler.setLevel(level)
        # message + appended traceback (Formatter appends exc_text automatically)
        handler.setFormatter(logging.Formatter("%(message)s"))

        # keep chatty libraries out of the sink to avoid feedback / noise
        for noisy in ("urllib3", "werkzeug", "kombu", "amqp", "psycopg2"):
            logging.getLogger(noisy).setLevel(logging.WARNING)

        root = logging.getLogger()
        if root.level == logging.NOTSET or root.level > level:
            root.setLevel(level)
        root.addHandler(handler)

        _DBSinkThread(q).start()
        _installed = True
        print(f"[db_logging] Postgres log sink active (service={service}, level={logging.getLevelName(level)})",
              file=sys.stderr)
        return True
