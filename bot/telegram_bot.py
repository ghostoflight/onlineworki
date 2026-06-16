"""
bot/telegram_bot.py — منصّة اختبار جودة (QA) ذاتية الخدمة عبر تلغرام

مبنيّة على Webhook + Flask، وكل حالة المحادثة مخزّنة في user_data (مفتاح موحّد
tg_state) لتعمل مع عدّة عمّال Gunicorn.

الميزات:
  • /start  : تسجيل تلقائي برصيد مجاني (+ ربط حساب قائم عبر /start <code>).
  • /profile: عرض بيئة المختبِر + أزرار (تحديث الجهاز Android/iOS، تحديث البروكسي).
  • /settings: تفعيل/إيقاف إشعارات نتائج الاختبار (notify_enabled).
  • /apps   : اختيار تطبيق ← إدخال القيمة ← متى التنفيذ؟ (فوري / جدولة مخصّصة).
  • التنفيذ يسحب بيانات المختبِر (OS, GAID/IDFA, AFID, Proxy) من قاعدة البيانات.
  • /balance /history /status /unlink /help.

كل مدخل نصّي محميّ بـ try/except مع تنظيف الحالة حتى لا يعلق المستخدم.

التوافق: register_webhook(app) و maybe_setup_webhook() كما هي (يستوردهما web.py).
"""
import os
import re
import json
import hashlib
import html
import logging
import threading
import time as _time
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from datetime import datetime, timezone

import requests
import psycopg2
import telebot
from telebot import types
from flask import request

import config
from db.connection import get_conn
from tasks.job_tasks import execute_job, _build_proxies, _log_event_history
import locales

logger = logging.getLogger(__name__)

try:
    from games_config import GAMES_DATA
except Exception:
    GAMES_DATA = []
    logger.warning("[Telegram] games_config.GAMES_DATA غير موجود — /apps فارغ")

bot: telebot.TeleBot | None = None
if config.TELEGRAM_BOT_TOKEN:
    bot = telebot.TeleBot(config.TELEGRAM_BOT_TOKEN, threaded=False)
    logger.info("[Telegram] bot instance created.")
else:
    logger.info("[Telegram] TELEGRAM_BOT_TOKEN not set — bot disabled.")

FREE_USES = 5
# خطوات تتطلّب إدخالاً نصّياً (تُلتقط بمعالج النص)
TEXT_STEPS = {"device_gaid", "device_idfa", "device_afid", "sniper_value", "custom_input",
              "proxy_host", "proxy_port", "proxy_user", "proxy_pass",
              "task_gaid", "task_idfa", "task_afid",
              "add_name", "add_package", "add_devkey", "add_events",
              "edit_value", "support_msg",
              "admin_user_credit", "admin_plan_edit",
              "admin_wallet", "admin_broadcast",
              "pw_key", "pw_en", "pw_ar", "pw_bn", "pw_credits", "pw_days", "pw_price"}


def _default_dev_key() -> str:
    return getattr(config, "DEFAULT_DEV_KEY", "") or os.environ.get("DEFAULT_DEV_KEY", "")


# ═══════════════════════════════════════════════════════════════════════════════
# In-memory user cache: tg_chat_id → user dict (TTL 60s)
# Thread-safe: dict.pop / dict.__setitem__ / dict.get are each atomic in CPython
# (GIL), which is sufficient for a TTL read-through cache. No data is lost if a
# race causes two threads to both miss the cache simultaneously — the worst
# outcome is two identical SELECT queries, both returning the same row.
# ═══════════════════════════════════════════════════════════════════════════════
_USER_CACHE: dict = {}          # {chat_id_str: (user_dict, expire_ts)}
_USER_CACHE_TTL = 60            # ثانية — قصير بما يكفي لتمييز تغييرات الرصيد


def _cache_set(chat_id, user):
    _USER_CACHE[str(chat_id)] = (user, _time.monotonic() + _USER_CACHE_TTL)


def _cache_get(chat_id):
    entry = _USER_CACHE.get(str(chat_id))
    if entry and entry[1] > _time.monotonic():
        return entry[0]
    return None


def _cache_del(chat_id):
    _USER_CACHE.pop(str(chat_id), None)


# ═══════════════════════════════════════════════════════════════════════════════
# مستخدمون
# ═══════════════════════════════════════════════════════════════════════════════
def _user_by_chat(chat_id) -> dict | None:
    cached = _cache_get(chat_id)
    if cached:
        return cached
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM users WHERE tg_chat_id = %s AND active = 1",
                (str(chat_id),)
            )
            user = cur.fetchone()
    if user:
        _cache_set(chat_id, user)
    return user


def _user_by_id(user_id) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            return cur.fetchone()


def _auto_register(chat_id, tg_username, first_name) -> dict:
    raw = (tg_username or first_name or f"tg{chat_id}").strip()
    base = re.sub(r"[^\w]", "_", raw)[:40] or f"tg{chat_id}"
    pw = hashlib.sha256(os.urandom(16)).hexdigest()
    for uname in (base, f"{base}_{chat_id}", f"{base}_{hashlib.sha1(os.urandom(4)).hexdigest()[:4]}"):
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO users (username, password, role, max_uses, uses_left, active, tg_chat_id)
                           VALUES (%s, %s, 'user', %s, %s, 1, %s) RETURNING *""",
                        (uname, pw, FREE_USES, FREE_USES, str(chat_id)),
                    )
                    return cur.fetchone()
        except psycopg2.IntegrityError:
            continue
    existing = _user_by_chat(chat_id)
    if existing:
        return existing
    raise RuntimeError("auto-register failed")


def _consume_link_code(code: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM user_data WHERE key='tg_link_code' AND value=%s", (code,))
            row = cur.fetchone()
            if not row:
                return None
            uid = row["user_id"]
            cur.execute("DELETE FROM user_data WHERE user_id=%s AND key='tg_link_code'", (uid,))
            return uid


def _set_chat(user_id, chat_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET tg_chat_id=%s WHERE id=%s", (str(chat_id), user_id))
    # Invalidate cache: the user row has changed (tg_chat_id assignment).
    _cache_del(chat_id)


def _clear_chat(chat_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET tg_chat_id=NULL WHERE tg_chat_id=%s", (str(chat_id),))
    # Invalidate cache: the user is now unlinked from this chat_id.
    _cache_del(chat_id)


# ═══════════════════════════════════════════════════════════════════════════════
# user_data: حالة + بيئة + إعدادات
# ═══════════════════════════════════════════════════════════════════════════════
def _ud_set(user_id, key, value):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO user_data (user_id, key, value, updated) VALUES (%s,%s,%s,NOW())
                   ON CONFLICT (user_id, key) DO UPDATE SET value=EXCLUDED.value, updated=NOW()""",
                (user_id, key, value),
            )


def _ud_get(user_id, key):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM user_data WHERE user_id=%s AND key=%s", (user_id, key))
            row = cur.fetchone()
    return row["value"] if row else None


def _ud_del(user_id, key):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM user_data WHERE user_id=%s AND key=%s", (user_id, key))


# ── الحالة (state machine) ───────────────────────────────────────────────────
def _set_state(user_id, step, data=None):
    _ud_set(user_id, "tg_state", json.dumps({"step": step, "data": data or {}}))


def _get_state(user_id):
    raw = _ud_get(user_id, "tg_state")
    if not raw:
        return None, {}
    try:
        obj = json.loads(raw)
        return obj.get("step"), obj.get("data", {})
    except Exception:
        return None, {}


def _clear_state(user_id):
    _ud_del(user_id, "tg_state")


# ── بيئة المختبِر ─────────────────────────────────────────────────────────────
def _get_env(user_id) -> dict:
    raw = _ud_get(user_id, "tg_env")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _save_env(user_id, **fields):
    """يحفظ الحقول غير الفارغة فقط (يتجاهل None و'' والمسافات) — يمنع تلويث البيئة."""
    env = _get_env(user_id)
    for k, v in fields.items():
        if v is not None and str(v).strip():
            env[k] = str(v).strip()
    _ud_set(user_id, "tg_env", json.dumps(env))


# ── إشعارات ───────────────────────────────────────────────────────────────────
def _get_notify(user_id) -> bool:
    return (_ud_get(user_id, "notify_enabled") or "1") == "1"


def _set_notify(user_id, enabled: bool):
    _ud_set(user_id, "notify_enabled", "1" if enabled else "0")


# ── i18n: لغة المستخدم محفوظة في user_data (مفتاح lang) ──────────────────────
def _lang(user_id) -> str:
    code = _ud_get(user_id, "lang")
    return code if code in locales.SUPPORTED else locales.DEFAULT_LANG


def _set_lang(user_id, code):
    if code in locales.SUPPORTED:
        _ud_set(user_id, "lang", code)


def _t(key, user_id, **kwargs):
    """t(key, user_id): يجلب الرسالة حسب لغة المستخدم من قاعدة البيانات."""
    return locales.lookup(key, _lang(user_id), **kwargs)


# ── تنقّل «رجوع»: مكدّس خطوات داخل tg_state (يحفظ الجلسة) ────────────────────
def _nav_push(user_id, step, data=None):
    """ينتقل لخطوة جديدة مع حفظ الخطوة الحالية في مكدّس التاريخ للرجوع."""
    cur_step, cur_data = _get_state(user_id)
    hist = (cur_data or {}).get("__hist", []) if cur_data else []
    new_data = dict(data or {})
    if cur_step:
        # خزّن لقطة الخطوة السابقة (بدون مكدّسها لتفادي التضخّم)
        snap = {k: v for k, v in (cur_data or {}).items() if k != "__hist"}
        hist = hist + [{"step": cur_step, "data": snap}]
    new_data["__hist"] = hist
    _set_state(user_id, step, new_data)


def _nav_back(user_id):
    """يرجع لخطوة سابقة من المكدّس. يعيد (step, data) أو (None, {}) إن فرغ."""
    _, data = _get_state(user_id)
    hist = (data or {}).get("__hist", [])
    if not hist:
        return None, {}
    prev = hist[-1]
    prev_data = dict(prev.get("data", {}))
    prev_data["__hist"] = hist[:-1]
    _set_state(user_id, prev["step"], prev_data)
    return prev["step"], prev_data


# ═══════════════════════════════════════════════════════════════════════════════
# الاشتراكات والمدفوعات (جداول جديدة) — المطلبان 7 و8
# ═══════════════════════════════════════════════════════════════════════════════
# Seed values only — the live source of truth is the `plans` table (admin-editable).
_DEFAULT_PLANS = {
    "w": {"days": 7,  "credits": 50,  "label_ar": "أسبوعي", "label_en": "Weekly",  "label_bn": "সাপ্তাহিক", "price": ""},
    "m": {"days": 30, "credits": 200, "label_ar": "شهري",   "label_en": "Monthly", "label_bn": "মাসিক",     "price": ""},
}


def _get_plans(active_only=True):
    """Ordered dict of plans from DB (falls back to the seed if the table is empty)."""
    _ensure_tables()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                q = "SELECT * FROM plans"
                if active_only:
                    q += " WHERE active=1"
                q += " ORDER BY sort ASC, key ASC"
                cur.execute(q)
                rows = cur.fetchall()
        if rows:
            return {r["key"]: dict(r) for r in rows}
    except Exception as e:
        logger.warning(f"[Telegram] load plans failed: {e}")
    return dict(_DEFAULT_PLANS)


def _get_plan(key):
    if not key:
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM plans WHERE key=%s", (key,))
                row = cur.fetchone()
        if row:
            return dict(row)
    except Exception:
        pass
    return _DEFAULT_PLANS.get(key)


def _update_plan(key, **fields):
    allowed = {"label_en", "label_ar", "label_bn", "credits", "days", "price", "active", "sort"}
    sets, vals = [], []
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k}=%s"); vals.append(v)
    if not sets:
        return False
    vals.append(key)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE plans SET {', '.join(sets)} WHERE key=%s", vals)
            return cur.rowcount > 0


def _create_plan(key):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO plans (key, label_en, label_ar, label_bn, credits, days, active, sort) "
                "VALUES (%s,%s,%s,%s,0,0,1,99) ON CONFLICT (key) DO NOTHING",
                (key, key, key, key),
            )
            return cur.rowcount > 0


def _create_plan_full(key, label_en, label_ar, label_bn, credits, days, price):
    """Insert a fully-specified plan (used by the guided Add-Plan wizard)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO plans (key, label_en, label_ar, label_bn, credits, days, price, active, sort)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,1,99)
                   ON CONFLICT (key) DO UPDATE SET
                     label_en=EXCLUDED.label_en, label_ar=EXCLUDED.label_ar, label_bn=EXCLUDED.label_bn,
                     credits=EXCLUDED.credits, days=EXCLUDED.days, price=EXCLUDED.price""",
                (key, label_en, label_ar, label_bn, int(credits), int(days), price),
            )
            return True


def _delete_plan(key) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM plans WHERE key=%s", (key,))


# ── Global app settings (e.g. crypto wallet address) ─────────────────────────
def _get_setting(key, default=""):
    _ensure_tables()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM app_settings WHERE key=%s", (key,))
                row = cur.fetchone()
        return row["value"] if row and row.get("value") else default
    except Exception:
        return default


def _set_setting(key, value) -> None:
    _ensure_tables()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO app_settings (key, value) VALUES (%s,%s) "
                "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
                (key, value),
            )


# ── Diagnostic / QA mode (per-user, persisted in user_data) ──────────────────
def _get_diag(uid) -> bool:
    return _ud_get(uid, "diag") == "1"


def _set_diag(uid, on) -> None:
    _ud_set(uid, "diag", "1" if on else "0")


def _proxy_scheme(s) -> str:
    """Extracts the scheme from a stored proxy URL (default http)."""
    s = (s or "").strip().lower()
    if "://" in s:
        sc = s.split("://", 1)[0]
        if sc in ("http", "https", "socks5", "socks5h", "socks4"):
            return sc
    return "http"


def _all_user_chat_ids():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tg_chat_id FROM users WHERE tg_chat_id IS NOT NULL AND tg_chat_id <> ''")
            return [r["tg_chat_id"] for r in cur.fetchall()]

_tables_ready = False


def _ensure_tables():
    """ينشئ جدولَي subscriptions و payments إن لم يوجدا (idempotent)."""
    global _tables_ready
    if _tables_ready:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS subscriptions (
                        id          SERIAL PRIMARY KEY,
                        user_id     INTEGER NOT NULL,
                        plan        TEXT,
                        status      TEXT DEFAULT 'active',
                        expires_at  TIMESTAMPTZ,
                        created_at  TIMESTAMPTZ DEFAULT NOW()
                    )""")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS payments (
                        id           SERIAL PRIMARY KEY,
                        user_id      INTEGER NOT NULL,
                        plan         TEXT,
                        screenshot   TEXT,
                        status       TEXT DEFAULT 'pending',
                        created_at   TIMESTAMPTZ DEFAULT NOW(),
                        reviewed_at  TIMESTAMPTZ
                    )""")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS support_tickets (
                        id         SERIAL PRIMARY KEY,
                        user_id    INTEGER NOT NULL,
                        message    TEXT,
                        status     TEXT DEFAULT 'unread',
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        read_at    TIMESTAMPTZ
                    )""")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS plans (
                        key       TEXT PRIMARY KEY,
                        label_en  TEXT DEFAULT '',
                        label_ar  TEXT DEFAULT '',
                        label_bn  TEXT DEFAULT '',
                        credits   INTEGER DEFAULT 0,
                        days      INTEGER DEFAULT 0,
                        price     TEXT DEFAULT '',
                        active    INTEGER DEFAULT 1,
                        sort      INTEGER DEFAULT 0
                    )""")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS app_settings (
                        key   TEXT PRIMARY KEY,
                        value TEXT DEFAULT ''
                    )""")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_sub_user ON subscriptions(user_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_pay_status ON payments(status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ticket_status ON support_tickets(status)")
                # seed default plans once (admin can edit/extend them afterwards)
                for i, (k, p) in enumerate(_DEFAULT_PLANS.items()):
                    cur.execute(
                        """INSERT INTO plans (key, label_en, label_ar, label_bn, credits, days, price, active, sort)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,1,%s) ON CONFLICT (key) DO NOTHING""",
                        (k, p["label_en"], p["label_ar"], p.get("label_bn", p["label_en"]),
                         p["credits"], p["days"], p.get("price", ""), i),
                    )
        _tables_ready = True
    except Exception as e:
        logger.warning(f"[Telegram] ensure tables failed: {e}")


def _plan_label(plan_key, uid=None):
    p = _get_plan(plan_key)
    if not p:
        return plan_key
    lang = _lang(uid) if uid else locales.DEFAULT_LANG
    label = p.get(f"label_{lang}") or p.get("label_en") or p.get("label_ar") or plan_key
    return label


def _has_active_subscription(user_id) -> bool:
    _ensure_tables()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM subscriptions WHERE user_id=%s AND status='active' AND expires_at > NOW() LIMIT 1",
                    (user_id,),
                )
                return cur.fetchone() is not None
    except Exception as e:
        logger.warning(f"[Telegram] sub check failed: {e}")
        return False


def _sub_expiry(user_id):
    """يعيد أحدث تاريخ انتهاء فعّال أو None."""
    _ensure_tables()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT expires_at FROM subscriptions
                   WHERE user_id=%s AND status='active' AND expires_at > NOW()
                   ORDER BY expires_at DESC LIMIT 1""",
                (user_id,),
            )
            row = cur.fetchone()
    return row["expires_at"] if row else None


def _create_payment(user_id, plan_key, screenshot_file_id) -> int | None:
    _ensure_tables()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO payments (user_id, plan, screenshot, status) VALUES (%s,%s,%s,'pending') RETURNING id",
                    (user_id, plan_key, screenshot_file_id),
                )
                return cur.fetchone()["id"]
    except Exception as e:
        logger.error(f"[Telegram] create payment failed: {e}")
        return None


def _get_payment(payment_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM payments WHERE id=%s", (payment_id,))
            return cur.fetchone()


def _list_pending_payments(limit=10):
    _ensure_tables()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM payments WHERE status='pending' ORDER BY id ASC LIMIT %s", (limit,)
            )
            return cur.fetchall()


def _grant_subscription(user_id, plan_key):
    """Activates a subscription and adds credits (called on payment approval/admin grant).
    Invalidates the user cache for every chat_id associated with this user so that
    the updated uses_left is reflected immediately on the next interaction.
    """
    p = _get_plan(plan_key)
    if not p:
        return None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO subscriptions (user_id, plan, status, expires_at)
                   VALUES (%s,%s,'active', NOW() + make_interval(days => %s))
                   RETURNING expires_at""",
                (user_id, plan_key, int(p["days"])),
            )
            expires = cur.fetchone()["expires_at"]
            cur.execute(
                "UPDATE users SET uses_left = uses_left + %s, max_uses = max_uses + %s WHERE id=%s",
                (int(p["credits"]), int(p["credits"]), user_id),
            )
    # Invalidate cache: uses_left / max_uses have changed for this user.
    # We must look up the chat_id from the DB since _grant_subscription only
    # receives user_id (not chat_id).
    try:
        target = _user_by_id(user_id)
        if target and target.get("tg_chat_id"):
            _cache_del(target["tg_chat_id"])
    except Exception as e:
        logger.warning(f"[Telegram] cache invalidation after grant failed: {e}")
    return expires


def _set_payment_status(payment_id, status):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE payments SET status=%s, reviewed_at=NOW() WHERE id=%s AND status='pending' RETURNING user_id, plan",
                (status, payment_id),
            )
            return cur.fetchone()


def _admin_chat_ids():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tg_chat_id FROM users WHERE role='admin' AND tg_chat_id IS NOT NULL")
            return [r["tg_chat_id"] for r in cur.fetchall()]


# ── Support tickets + admin counters (silent inbox, no push spam) ────────────
def _create_ticket(user_id, message) -> int | None:
    _ensure_tables()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO support_tickets (user_id, message, status) VALUES (%s,%s,'unread') RETURNING id",
                    (user_id, message),
                )
                return cur.fetchone()["id"]
    except Exception as e:
        logger.error(f"[Telegram] create ticket failed: {e}")
        return None


def _count_pending_payments() -> int:
    _ensure_tables()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS n FROM payments WHERE status='pending'")
                return int(cur.fetchone()["n"])
    except Exception:
        return 0


def _count_unread_tickets() -> int:
    _ensure_tables()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS n FROM support_tickets WHERE status='unread'")
                return int(cur.fetchone()["n"])
    except Exception:
        return 0


def _next_unread_ticket():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM support_tickets WHERE status='unread' ORDER BY id ASC LIMIT 1")
            return cur.fetchone()


def _mark_ticket_read(tid) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE support_tickets SET status='read', read_at=NOW() WHERE id=%s", (tid,))


def _home_btn(uid):
    """A universal '🏠 Main menu' button — drop into any deep menu to avoid dead ends."""
    return types.InlineKeyboardButton(_t("btn_home", uid), callback_data="home")


def _count_users() -> int:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS n FROM users")
                return int(cur.fetchone()["n"])
    except Exception:
        return 0


def _list_users(offset=0, limit=8):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, username, role, uses_left, max_uses FROM users ORDER BY id ASC OFFSET %s LIMIT %s",
                (offset, limit),
            )
            return cur.fetchall()


def _admin_add_credits(uid, n) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET uses_left = GREATEST(0, uses_left + %s), "
                "max_uses = GREATEST(max_uses, uses_left + %s) WHERE id=%s",
                (n, n, uid),
            )
    # Invalidate cache: uses_left has changed.
    try:
        target = _user_by_id(uid)
        if target and target.get("tg_chat_id"):
            _cache_del(target["tg_chat_id"])
    except Exception as e:
        logger.warning(f"[Telegram] cache invalidation after add_credits failed: {e}")


def _delete_user(uid) -> None:
    """Removes a user and all dependent rows (FK-cascade + the FK-less tables)."""
    # Fetch the chat_id before deletion so we can evict the cache entry.
    try:
        target = _user_by_id(uid)
        chat_id = target.get("tg_chat_id") if target else None
    except Exception:
        chat_id = None

    with get_conn() as conn:
        with conn.cursor() as cur:
            for t in ("subscriptions", "payments", "support_tickets"):
                cur.execute(f"DELETE FROM {t} WHERE user_id=%s", (uid,))
            cur.execute("DELETE FROM users WHERE id=%s", (uid,))   # cascades the rest

    if chat_id:
        _cache_del(chat_id)


# ── Admin dashboard renderers (module-level; HTML, each ends with a Home btn) ─
def _open_admin(chat_id, u):
    np_, ns_, nu_ = _count_pending_payments(), _count_unread_tickets(), _count_users()
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton(_t("btn_admin_payments", u["id"], n=np_), callback_data="adm:pay"))
    kb.row(types.InlineKeyboardButton(_t("btn_admin_support", u["id"], n=ns_), callback_data="adm:sup"))
    kb.row(types.InlineKeyboardButton(_t("btn_admin_users", u["id"], n=nu_), callback_data="adm:users:0"))
    kb.row(types.InlineKeyboardButton(_t("btn_admin_plans", u["id"]), callback_data="adm:plans"))
    kb.row(types.InlineKeyboardButton(_t("btn_admin_payset", u["id"]), callback_data="adm:payset"))
    kb.row(types.InlineKeyboardButton(_t("btn_admin_broadcast", u["id"]), callback_data="adm:bc"))
    kb.row(_home_btn(u["id"]))
    bot.send_message(chat_id, _t("admin_menu_title", u["id"]), parse_mode="HTML", reply_markup=kb)


def _admin_show_payset(chat_id, u):
    wallet = _get_setting("wallet_address", "—")
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton(_t("btn_edit_wallet", u["id"]), callback_data="adm:setwallet"))
    kb.row(_home_btn(u["id"]))
    bot.send_message(chat_id, _t("admin_payset_view", u["id"], wallet=html.escape(str(wallet))),
                     parse_mode="HTML", reply_markup=kb)


def _admin_next_payment(chat_id, u):
    pend = _list_pending_payments(limit=1)
    if not pend:
        bot.send_message(chat_id, _t("no_pending_pay", u["id"]))
        _open_admin(chat_id, u)
        return
    pay = pend[0]
    target = _user_by_id(pay["user_id"])
    cap = _t("admin_pay_request", u["id"], id=pay["id"],
             user=(target["username"] if target else pay["user_id"]), plan=_plan_label(pay["plan"], u["id"]))
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(_t("btn_approve", u["id"]), callback_data=f"pay:approve:{pay['id']}"),
        types.InlineKeyboardButton(_t("btn_reject", u["id"]),  callback_data=f"pay:reject:{pay['id']}"),
    )
    kb.row(types.InlineKeyboardButton(_t("btn_next", u["id"]), callback_data="adm:pay"), _home_btn(u["id"]))
    try:
        bot.send_photo(chat_id, pay["screenshot"], caption=cap, reply_markup=kb)
    except Exception:
        bot.send_message(chat_id, cap, reply_markup=kb)


def _admin_next_ticket(chat_id, u):
    t = _next_unread_ticket()
    if not t:
        bot.send_message(chat_id, _t("admin_no_support", u["id"]))
        _open_admin(chat_id, u)
        return
    _mark_ticket_read(t["id"])
    target = _user_by_id(t["user_id"])
    when = t["created_at"].strftime("%Y-%m-%d %H:%M") if hasattr(t["created_at"], "strftime") else str(t["created_at"])
    txt = _t("support_ticket_view", u["id"], id=t["id"],
             user=html.escape(str(target["username"] if target else "?")), uid=t["user_id"],
             when=when, text=html.escape(str(t["message"] or "")))
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton(_t("btn_next", u["id"]), callback_data="adm:sup"), _home_btn(u["id"]))
    bot.send_message(chat_id, txt, parse_mode="HTML", reply_markup=kb)


def _admin_show_users(chat_id, u, offset=0):
    rows = _list_users(offset=offset, limit=8)
    kb = types.InlineKeyboardMarkup()
    for r in rows:
        kb.row(types.InlineKeyboardButton(
            f"#{r['id']} {r['username']} · {r['uses_left']}/{r['max_uses']}",
            callback_data=f"adm:user:{r['id']}"))
    nav = []
    if offset > 0:
        nav.append(types.InlineKeyboardButton(_t("btn_prev", u["id"]), callback_data=f"adm:users:{max(0, offset-8)}"))
    if len(rows) == 8:
        nav.append(types.InlineKeyboardButton(_t("btn_more", u["id"]), callback_data=f"adm:users:{offset+8}"))
    if nav:
        kb.row(*nav)
    kb.row(_home_btn(u["id"]))
    bot.send_message(chat_id, _t("admin_users_title", u["id"]), parse_mode="HTML", reply_markup=kb)


def _admin_show_user(chat_id, u, target_uid):
    t = _user_by_id(target_uid)
    if not t:
        _admin_show_users(chat_id, u, 0)
        return
    txt = _t("admin_user_view", u["id"], username=html.escape(str(t["username"])), uid=t["id"],
             role=t["role"], left=t["uses_left"], max=t["max_uses"])
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton(_t("btn_add_credits", u["id"]), callback_data=f"adm:ucredit:{t['id']}"),
           types.InlineKeyboardButton(_t("btn_grant_plan", u["id"]), callback_data=f"adm:uplan:{t['id']}"))
    kb.row(types.InlineKeyboardButton(_t("btn_del_user", u["id"]), callback_data=f"adm:udel:{t['id']}"))
    kb.row(types.InlineKeyboardButton(_t("btn_admin_users", u["id"], n=_count_users()), callback_data="adm:users:0"),
           _home_btn(u["id"]))
    bot.send_message(chat_id, txt, parse_mode="HTML", reply_markup=kb)


def _admin_grant_menu(chat_id, u, target_uid):
    kb = types.InlineKeyboardMarkup()
    for key, p in _get_plans(active_only=False).items():
        kb.row(types.InlineKeyboardButton(
            f"{_plan_label(key, u['id'])} · {p['credits']}/{p['days']}d",
            callback_data=f"adm:ugrant:{target_uid}:{key}"))
    kb.row(types.InlineKeyboardButton(_t("btn_back", u["id"]), callback_data=f"adm:user:{target_uid}"))
    bot.send_message(chat_id, _t("choose_plan", u["id"]), reply_markup=kb)


def _admin_show_plans(chat_id, u):
    kb = types.InlineKeyboardMarkup()
    for key, p in _get_plans(active_only=False).items():
        flag = "✅" if p.get("active", 1) else "⛔"
        kb.row(types.InlineKeyboardButton(f"{flag} {key} · {_plan_label(key, u['id'])}",
                                          callback_data=f"adm:plan:{key}"))
    kb.row(types.InlineKeyboardButton(_t("btn_add_plan", u["id"]), callback_data="adm:padd"))
    kb.row(_home_btn(u["id"]))
    bot.send_message(chat_id, _t("admin_plans_title", u["id"]), parse_mode="HTML", reply_markup=kb)


def _admin_show_plan(chat_id, u, key):
    p = _get_plan(key)
    if not p:
        _admin_show_plans(chat_id, u)
        return
    txt = _t("admin_plan_view", u["id"], key=key, label=html.escape(str(_plan_label(key, u["id"]))),
             credits=p["credits"], days=p["days"], price=html.escape(str(p.get("price") or "—")),
             active=("✅" if p.get("active", 1) else "⛔"))
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton(_t("btn_edit_credits", u["id"]), callback_data=f"adm:pedit:{key}:credits"),
           types.InlineKeyboardButton(_t("btn_edit_days", u["id"]), callback_data=f"adm:pedit:{key}:days"))
    kb.row(types.InlineKeyboardButton(_t("btn_edit_price", u["id"]), callback_data=f"adm:pedit:{key}:price"),
           types.InlineKeyboardButton(_t("btn_toggle_active", u["id"]), callback_data=f"adm:ptoggle:{key}"))
    kb.row(types.InlineKeyboardButton(_t("btn_edit_label", u["id"], lang="EN"), callback_data=f"adm:pedit:{key}:label_en"),
           types.InlineKeyboardButton(_t("btn_edit_label", u["id"], lang="AR"), callback_data=f"adm:pedit:{key}:label_ar"),
           types.InlineKeyboardButton(_t("btn_edit_label", u["id"], lang="BN"), callback_data=f"adm:pedit:{key}:label_bn"))
    kb.row(types.InlineKeyboardButton(_t("btn_del_plan", u["id"]), callback_data=f"adm:pdel:{key}"))
    kb.row(types.InlineKeyboardButton(_t("btn_admin_plans", u["id"]), callback_data="adm:plans"), _home_btn(u["id"]))
    bot.send_message(chat_id, txt, parse_mode="HTML", reply_markup=kb)


# ═══════════════════════════════════════════════════════════════════════════════
# الرصيد
# ═══════════════════════════════════════════════════════════════════════════════
def _consume_use(user):
    if user["role"] == "admin":
        return True, user["uses_left"]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET uses_left = uses_left - 1 WHERE id=%s AND uses_left > 0 RETURNING uses_left",
                (user["id"],),
            )
            row = cur.fetchone()
    # Invalidate cache: uses_left has been decremented (or the update was a no-op,
    # but evicting on no-op is harmless — just one extra SELECT on next access).
    if user.get("tg_chat_id"):
        _cache_del(user["tg_chat_id"])
    return (True, row["uses_left"]) if row else (False, 0)


def _refund_use(user):
    if user["role"] == "admin":
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET uses_left = uses_left + 1 WHERE id=%s", (user["id"],))
    # Invalidate cache: uses_left has been incremented.
    if user.get("tg_chat_id"):
        _cache_del(user["tg_chat_id"])


# ═══════════════════════════════════════════════════════════════════════════════
# Proxy helpers
# ═══════════════════════════════════════════════════════════════════════════════
def _proxies_from_string(s):
    s = (s or "").strip()
    if not s:
        return None
    url = s if "://" in s else f"http://{s}"
    return {"http": url, "https": url}


def _proxy_parts(s):
    s = (s or "").strip()
    if not s:
        return "", "", "", ""
    if "://" in s:
        s = s.split("://", 1)[1]
    user = pw = ""
    if "@" in s:
        creds, _, hostport = s.partition("@")
        user, _, pw = creds.partition(":")
    else:
        hostport = s
    host, _, port = hostport.partition(":")
    return host, port, user, pw


def _build_proxy_url(scheme, host, port, user="", pw=""):
    """Builds scheme://[user:pass@]host:port with URL-encoded credentials."""
    from urllib.parse import quote
    scheme = (scheme or "http").lower()
    creds = ""
    if user:
        creds = f"{quote(str(user), safe='')}:{quote(str(pw or ''), safe='')}@"
    return f"{scheme}://{creds}{host}:{port}"


def _finish_proxy(chat_id, u, data):
    """Assembles the proxy URL from wizard data, saves it, returns to main menu."""
    url = _build_proxy_url(data.get("scheme"), data.get("host", ""), data.get("port", ""),
                           data.get("user", ""), data.get("pass", ""))
    _save_env(u["id"], proxy=url)
    _clear_state(u["id"])
    kb = types.InlineKeyboardMarkup()
    kb.row(_home_btn(u["id"]))
    bot.send_message(chat_id, _t("proxy_saved", u["id"]) + f"\n<code>{html.escape(url)}</code>",
                     parse_mode="HTML", reply_markup=kb)


# ═══════════════════════════════════════════════════════════════════════════════
# إرسال الحدث — يسحب بيانات المختبِر ديناميكياً
# ═══════════════════════════════════════════════════════════════════════════════
def _missing_requirements(app_cfg, env):
    """
    بوابة الصدّ: تعيد قائمة الحقول الناقصة لتنفيذ اختبار صالح.
    قائمة فارغة = جاهز. تمنع تلويث scheduled_jobs أو إرسال Payload فارغ.
    """
    missing = []
    if not (app_cfg.get("dev_key") or _default_dev_key() or "").strip():
        missing.append("dev_key")            # admin-side config (not user)
    os_ = (env.get("os") or "").strip().lower()
    if os_ not in ("android", "ios"):
        missing.append("OS")
    elif os_ == "ios" and not (env.get("idfa") or "").strip():
        missing.append("IDFA")
    elif os_ == "android" and not (env.get("gaid") or "").strip():
        missing.append("GAID")
    if not (env.get("afid") or "").strip():
        missing.append("AFID")
    return missing


def _requirements_message(missing, uid=0):
    """User-facing message: admin dev_key gap vs. user device-config gap."""
    if "dev_key" in missing:
        return _t("app_not_ready", uid)
    return _t("profile_incomplete", uid) + " " + ", ".join(missing)


def _dispatch_event(app_cfg, value, user, env):
    """
    Sends one in-app event for a SPECIFIC package. Returns (ok, info, transport_error).

    Note: the dev_key is package-specific (each app in games_config carries its own
    dev_key). We resolve it strictly from app_cfg for the requested package — never a
    global key — then fall back to the admin default only if the app omits one.
    """
    package = app_cfg["package"]
    dev_key = app_cfg.get("dev_key") or _default_dev_key()
    if not (dev_key or "").strip():
        # Defensive: never POST without a key. transport_error=True ⇒ credit refunded.
        logger.warning(f"[Dispatch] aborted: missing dev_key for package={package}")
        return False, "missing dev_key", True

    # User input (value) is the explicit eventName; otherwise the app's default event.
    event_name = (value or "").strip() or app_cfg.get("event", "af_level_achieved")
    os_ = (env.get("os") or "").lower()

    body = {
        "appsflyer_id": env.get("afid", "") or app_cfg.get("afid", ""),
        "eventName": event_name,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "eventValue": "{}",
    }
    if os_ == "ios":
        body["idfa"] = env.get("idfa", "")
    else:
        body["advertising_id"] = env.get("gaid", "") or app_cfg.get("gaid", "")

    proxies = _proxies_from_string(env.get("proxy", "")) or _build_proxies(
        app_cfg.get("proxy_host", ""), app_cfg.get("proxy_port", ""),
        app_cfg.get("proxy_user", ""), app_cfg.get("proxy_pass", ""),
    )

    url = f"https://api2.appsflyer.com/inappevent/{package}"
    headers = {"Content-Type": "application/json", "authentication": dev_key or ""}

    # ── Deep debug: exact URL / headers (key masked) / payload, right before POST ──
    masked = (dev_key[:4] + "…" + str(len(dev_key)) + "c") if dev_key else "<none>"
    logger.debug(
        "[Dispatch] POST %s | headers={Content-Type:application/json, authentication:%s} | "
        "payload=%s | proxied=%s | user=%s",
        url, masked, json.dumps(body, ensure_ascii=False), bool(proxies), user.get("id"),
    )
    if not any(body.get(k) for k in ("advertising_id", "idfa")):
        logger.error("[Dispatch] payload has NO device identifier (gaid/idfa empty) for package=%s "
                     "— event will likely be rejected. env=%s", package, {k: env.get(k) for k in ("os", "gaid", "idfa", "afid")})

    try:
        r = requests.post(url, headers=headers, json=body, proxies=proxies, timeout=15)
        ok = r.status_code in (200, 201)
        if not ok:
            logger.error("[Dispatch] package=%s status=%s resp=%s", package, r.status_code, (r.text or "")[:200])
        _log_event_history(user["id"], package, event_name, r.status_code, ok)
        return ok, f"HTTP {r.status_code}", False
    except requests.RequestException as e:
        logger.error("[Dispatch] transport error package=%s: %s", package, e)
        _log_event_history(user["id"], package, event_name, 0, False)
        return False, str(e)[:80], True


def _safe_bg(target, args, kwargs):
    try:
        target(*args, **kwargs)
    except Exception as e:
        logger.error("[BG] background task failed: %s", e)


def _bg(target, *args, **kwargs):
    """Run a blocking task off the webhook thread so Telegram gets its 200 OK instantly."""
    threading.Thread(target=_safe_bg, args=(target, args, kwargs), daemon=True).start()


def _do_execute_now(chat_id, user, idx, value, env=None):
    if idx < 0 or idx >= len(GAMES_DATA):
        bot.send_message(chat_id, _t("session_apps", user["id"]))
        return
    app_cfg = GAMES_DATA[idx]
    if env is None:                       # data isolation: env may come from the task itself
        env = _get_env(user["id"])
    # validation gate (fast) — before consuming any credit
    missing = _missing_requirements(app_cfg, env)
    if missing:
        bot.send_message(chat_id, _requirements_message(missing, user["id"]))
        return

    # ── Diagnostic / QA mode: build and show the payload, send nothing, no credit ──
    if _get_diag(user["id"]):
        uid = user["id"]
        dev_key = app_cfg.get("dev_key") or _default_dev_key() or ""
        event_name = (value or "").strip() or app_cfg.get("event", "af_level_achieved")
        os_ = (env.get("os") or "").lower()
        body = {
            "appsflyer_id": env.get("afid", "") or app_cfg.get("afid", ""),
            "eventName": event_name,
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "eventValue": "{}",
        }
        if os_ == "ios":
            body["idfa"] = env.get("idfa", "")
        else:
            body["advertising_id"] = env.get("gaid", "") or app_cfg.get("gaid", "")
        masked = (dev_key[:4] + "…" + str(len(dev_key)) + "c") if dev_key else "<none>"
        url = f"https://api2.appsflyer.com/inappevent/{app_cfg['package']}"
        headers_view = f"Content-Type: application/json | authentication: {masked}"
        bot.send_message(
            chat_id,
            _t("diag_preview", uid, url=html.escape(url), headers=html.escape(headers_view),
               payload=html.escape(json.dumps(body, ensure_ascii=False, indent=2))),
            parse_mode="HTML",
        )
        return

    # credit/subscription gate (fast DB) — active subscription bypasses consumption
    consumed = False
    if _has_active_subscription(user["id"]):
        left = user["uses_left"]
    else:
        ok_bal, left = _consume_use(user)
        if not ok_bal:
            bot.send_message(chat_id, _t("no_credits", user["id"]))
            return
        consumed = True
    bot.send_chat_action(chat_id, "typing")

    uid = user["id"]
    ev_label = html.escape(str(value or app_cfg.get("event", "")))

    # The actual HTTP send is the only slow part — run it off-thread so the
    # webhook returns immediately; the result message is sent from the thread.
    def _worker():
        ok, info, transport_err = _dispatch_event(app_cfg, value, user, env)
        bal = left
        if not ok and transport_err and consumed:
            _refund_use(user)
            bal += 1
        if ok:
            bot.send_message(chat_id, _t("exec_ok", uid, event=ev_label, left=bal), parse_mode="HTML")
        else:
            bot.send_message(chat_id, _t("exec_fail", uid, info=html.escape(str(info)), left=bal), parse_mode="HTML")

    _bg(_worker)


_DELAY_RE = re.compile(r"^\s*(\d+)\s*([hHdD])\s*$")


def _parse_delay_minutes(text):
    m = _DELAY_RE.match(text or "")
    if not m:
        return None
    n, unit = int(m.group(1)), m.group(2).lower()
    if n <= 0:
        return None
    return n * 60 if unit == "h" else n * 1440


def _parse_custom(text):
    """
    وضع المخصّص: "Event | Hours" → (event, minutes) أو None.
    الجزء الأيمن يقبل رقم ساعات (24) أو صيغة التأخير (24h / 3d).
    """
    if "|" not in (text or ""):
        return None
    left, right = text.split("|", 1)
    event = left.strip()
    right = right.strip()
    if not event:
        return None
    if re.fullmatch(r"\d+", right):
        h = int(right)
        if h <= 0:
            return None
        return event, h * 60
    minutes = _parse_delay_minutes(right)   # يدعم 24h / 3d أيضاً
    return (event, minutes) if minutes else None


def _send_mode_menu(chat_id, app_cfg, uid=0):
    """Execution-mode picker (HTML) — Sniper / Custom + back/cancel, fully localized."""
    text = _t("mode_menu_text", uid, app=html.escape(app_cfg["name"]))
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(_t("btn_mode_sniper", uid), callback_data="mode:sniper"),
        types.InlineKeyboardButton(_t("btn_mode_custom", uid), callback_data="mode:custom"),
    )
    kb.row(
        types.InlineKeyboardButton(_t("btn_back", uid), callback_data="nav:back"),
        types.InlineKeyboardButton(_t("btn_cancel", uid), callback_data="nav:cancel"),
    )
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)


# ── الكتالوج المتسلسل (يُحسب من GAMES_DATA — يدعم البنية الجديدة os/cat) ──────
def _os_list():
    seen = []
    for g in GAMES_DATA:
        o = g.get("os", "android")
        if o not in seen:
            seen.append(o)
    return seen


def _cat_list(os_):
    cats = []
    for g in GAMES_DATA:
        if g.get("os") == os_ and g.get("cat") and g["cat"] not in cats:
            cats.append(g["cat"])
    return cats


def _game_list(os_, cat):
    return [(i, g) for i, g in enumerate(GAMES_DATA)
            if g.get("os") == os_ and g.get("cat") == cat]


def _env_from_data(data):
    """عزل البيانات: يبني env المهمة من بيانات الحالة (لا من الملف العام)."""
    os_ = (data.get("os") or "android").lower()
    env = {"os": os_, "afid": data.get("afid", "")}
    if os_ == "ios":
        env["idfa"] = data.get("idfa", "")
    else:
        env["gaid"] = data.get("gaid", "")
    return env


def _nav_row(uid, with_back=True):
    kb_row = []
    if with_back:
        kb_row.append(types.InlineKeyboardButton(_t("btn_back", uid), callback_data="nav:back"))
    kb_row.append(types.InlineKeyboardButton(_t("btn_cancel", uid), callback_data="nav:cancel"))
    return kb_row


# ── لوحات العرض (نقيّة: لا تغيّر الحالة) ──────────────────────────────────────
_OS_LABELS = {"android": "🤖 Android", "ios": "🍎 iOS"}


def _render_os_ui(chat_id, u):
    kb = types.InlineKeyboardMarkup()
    row = [types.InlineKeyboardButton(_OS_LABELS.get(o, o), callback_data=f"nav:os:{o}")
           for o in _os_list()]
    if row:
        kb.row(*row)
    kb.row(*_nav_row(u["id"], with_back=False))
    bot.send_message(chat_id, _t("choose_os", u["id"]), reply_markup=kb)


def _render_categories_ui(chat_id, u, os_):
    cats = _cat_list(os_)
    kb = types.InlineKeyboardMarkup()
    for ci, cat in enumerate(cats):
        kb.row(types.InlineKeyboardButton(cat, callback_data=f"nav:cat:{ci}"))
    kb.row(*_nav_row(u["id"]))
    bot.send_message(chat_id, _t("choose_category", u["id"]), reply_markup=kb)


def _render_games_ui(chat_id, u, os_, cat):
    games = _game_list(os_, cat)
    if not games:
        bot.send_message(chat_id, _t("no_games", u["id"]))
        return
    kb = types.InlineKeyboardMarkup()
    for ai, g in games:
        kb.row(types.InlineKeyboardButton(g["name"], callback_data=f"nav:game:{ai}"))
    kb.row(*_nav_row(u["id"]))
    bot.send_message(chat_id, _t("choose_game", u["id"]), reply_markup=kb)


_DEVICE_PROMPT = {"task_gaid": "ask_gaid", "task_idfa": "ask_idfa", "task_afid": "ask_afid"}


def _ask_device(chat_id, u, step):
    kb = types.InlineKeyboardMarkup()
    kb.row(*_nav_row(u["id"]))
    bot.send_message(chat_id, _t(_DEVICE_PROMPT[step], u["id"]), parse_mode="HTML", reply_markup=kb)


def _render_step(chat_id, u, step, data):
    """يعيد رسم واجهة خطوة معيّنة (يُستخدم مع زر الرجوع)."""
    if step == "apps_os":
        _render_os_ui(chat_id, u)
    elif step == "apps_cat":
        _render_categories_ui(chat_id, u, data.get("os"))
    elif step == "apps_game":
        _render_games_ui(chat_id, u, data.get("os"), data.get("cat"))
    elif step in ("task_gaid", "task_idfa", "task_afid"):
        _ask_device(chat_id, u, step)
    elif step == "awaiting_mode":
        idx = data.get("app_index", -1)
        if 0 <= idx < len(GAMES_DATA):
            _send_mode_menu(chat_id, GAMES_DATA[idx], u["id"])


def _schedule_test(user, idx, value, minutes, env=None):
    """
    يجدول اختباراً. يعيد (status, payload):
      ("ok", run_at) | ("invalid", missing_list) | ("balance", None)
      | ("session", None) | ("db", None)
    يتحقّق من الجاهزية قبل خصم الرصيد أو الإدراج (لا تلويث لقاعدة البيانات).
    env اختياري: بيانات الجهاز الخاصة بالمهمة (عزل البيانات)؛ وإلا من الملف العام.
    """
    if idx < 0 or idx >= len(GAMES_DATA):
        return "session", None
    app_cfg = GAMES_DATA[idx]
    if env is None:
        env = _get_env(user["id"])

    missing = _missing_requirements(app_cfg, env)
    if missing:
        return "invalid", missing

    # بوابة الرصيد/الاشتراك: الاشتراك الفعّال يتجاوز خصم الرصيد
    if not _has_active_subscription(user["id"]):
        ok_bal, _ = _consume_use(user)
        if not ok_bal:
            return "balance", None

    # إصلاح منطقي: القيمة الممرّرة (value) هي eventName الصريح، وإلا الافتراضي
    event_name = (value or "").strip() or app_cfg.get("event", "af_level_achieved")
    dev_key = app_cfg.get("dev_key") or _default_dev_key()
    os_ = (env.get("os") or "").lower()
    device_id = env.get("idfa", "") if os_ == "ios" else env.get("gaid", "")
    p_host, p_port, p_user, p_pass = _proxy_parts(env.get("proxy", ""))
    p_scheme = _proxy_scheme(env.get("proxy", ""))
    name = f"{app_cfg['name']} · {event_name}"
    events = json.dumps([{"name": event_name}])

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO scheduled_jobs
                         (user_id, name, events, package, dev_key, gaid, afid, os,
                          proxy_host, proxy_port, proxy_user, proxy_pass, proxy_scheme,
                          run_at, enabled)
                       VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                               NOW() + make_interval(mins => %s), 1)
                       RETURNING run_at""",
                    (user["id"], name, events, app_cfg["package"], dev_key,
                     device_id, env.get("afid", ""), os_,
                     p_host, p_port, p_user, p_pass, p_scheme, minutes),
                )
                run_at = cur.fetchone()["run_at"]
        return "ok", run_at
    except Exception as e:
        _refund_use(user)   # فشل الإدراج — أعد الرصيد
        logger.error(f"[Telegram] schedule failed: {e}")
        return "db", None


# ═══════════════════════════════════════════════════════════════════════════════
# Decorator
# ═══════════════════════════════════════════════════════════════════════════════
def linked(handler):
    @wraps(handler)
    def wrap(message):
        user = _user_by_chat(message.chat.id)
        if not user:
            bot.reply_to(message, locales.lookup("need_start"))
            return
        return handler(message, user)
    return wrap


# ═══════════════════════════════════════════════════════════════════════════════
# Handlers
# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
# مسار /add — إنشاء مهمة مجدولة بخطوات (يطابق تصميم الواجهة) + القائمة الرئيسية
# ═══════════════════════════════════════════════════════════════════════════════
_ADD_EX = {
    "add_package": ["com.example.app", "com.game.mobile"],
    "add_events":  ["af_launch,af_login", "af_purchase,af_complete"],
}
_ADD_PREV = {
    "add_package": "add_name",
    "add_devkey":  "add_package",
    "add_events":  "add_devkey",
    "awaiting_confirm": "add_events",
}


def _step_bar(step_no, total=5):
    return "▰" * step_no + "▱" * (total - step_no)


def _send_add_step(chat_id, step, data, err=None, uid=0):
    """Sends the step bubble (HTML) + keyboard. err optional for failed validation."""
    kb = types.InlineKeyboardMarkup()
    prefix = f"⚠️ {html.escape(err)}\n\n" if err else ""
    cancel_btn = types.InlineKeyboardButton(_t("btn_cancel", uid), callback_data="add:cancel")
    back_btn = types.InlineKeyboardButton(_t("btn_back", uid), callback_data="add:back")
    if step == "add_name":
        text = prefix + _t("add_s1", uid, bar=_step_bar(1))
        kb.row(cancel_btn)
    elif step == "add_package":
        text = prefix + _t("add_s2", uid, bar=_step_bar(2))
        for ex in _ADD_EX["add_package"]:
            kb.row(types.InlineKeyboardButton(ex, callback_data=f"add:exv:{ex}"))
        kb.row(back_btn, cancel_btn)
    elif step == "add_devkey":
        text = prefix + _t("add_s3", uid, bar=_step_bar(3))
        kb.row(back_btn, cancel_btn)
    elif step == "add_events":
        text = prefix + _t("add_s4", uid, bar=_step_bar(4))
        for ex in _ADD_EX["add_events"]:
            kb.row(types.InlineKeyboardButton(ex, callback_data=f"add:exv:{ex}"))
        kb.row(back_btn, cancel_btn)
    else:
        return
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)


def _send_review(chat_id, user, data):
    """Step 5: review the task with a payload preview, fully localized."""
    uid = user["id"]
    env = _get_env(uid)
    os_ = (env.get("os") or "").lower()
    afid = env.get("afid", "") or "—"
    dev_id = (env.get("idfa", "") if os_ == "ios" else env.get("gaid", "")) or "—"
    id_field = "idfa" if os_ == "ios" else "advertising_id"
    first_ev = data["events"][0] if data.get("events") else "af_event"

    name     = html.escape(data.get("name", ""))
    package  = html.escape(data.get("package", ""))
    dev_mask = html.escape(data.get("dev_key", "")[:6] + "…") if data.get("dev_key") else "—"
    events_s = html.escape(", ".join(data.get("events", [])))

    payload_raw = (
        "{\n"
        f'  "appsflyer_id": "{afid}",\n'
        f'  "eventName": "{first_ev}",\n'
        '  "eventTime": "<auto>",\n'
        '  "eventValue": "{}",\n'
        f'  "{id_field}": "{dev_id}"\n'
        "}"
    )
    text = _t("add_review", uid, bar=_step_bar(5), name=name, package=package,
              devkey=dev_mask, events=events_s, payload=html.escape(payload_raw))
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton(_t("btn_save_task", uid), callback_data="add:save"),
           types.InlineKeyboardButton(_t("btn_restart", uid), callback_data="add:restart"))
    kb.row(types.InlineKeyboardButton(_t("btn_cancel", uid), callback_data="add:cancel"))
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)


def _add_advance(chat_id, user, value):
    """Handles the current /add step input (text or example button) with validation."""
    uid = user["id"]
    step, data = _get_state(uid)
    value = (value or "").strip()
    if step == "add_name":
        if not re.fullmatch(r"[A-Za-z0-9_]{2,40}", value):
            _send_add_step(chat_id, "add_name", data, err=_t("add_err_name", uid), uid=uid)
            return
        data["name"] = value
        _set_state(uid, "add_package", data)
        _send_add_step(chat_id, "add_package", data, uid=uid)
    elif step == "add_package":
        if "." not in value or not re.fullmatch(r"[A-Za-z0-9_.]{3,80}", value):
            _send_add_step(chat_id, "add_package", data, err=_t("add_err_package", uid), uid=uid)
            return
        data["package"] = value
        _set_state(uid, "add_devkey", data)
        _send_add_step(chat_id, "add_devkey", data, uid=uid)
    elif step == "add_devkey":
        if len(value) < 6:
            _send_add_step(chat_id, "add_devkey", data, err=_t("add_err_devkey", uid), uid=uid)
            return
        data["dev_key"] = value
        _set_state(uid, "add_events", data)
        _send_add_step(chat_id, "add_events", data, uid=uid)
    elif step == "add_events":
        evs = [e.strip() for e in value.split(",") if e.strip()]
        if not evs:
            _send_add_step(chat_id, "add_events", data, err=_t("add_err_events", uid), uid=uid)
            return
        data["events"] = evs
        _set_state(uid, "awaiting_confirm", data)
        _send_review(chat_id, user, data)


def _save_add_job(user, data):
    """يُدرج المهمة في scheduled_jobs (نفس منطق DB). run_at=NOW() ⇒ تُنفَّذ بأقرب مسح."""
    env = _get_env(user["id"])
    os_ = (env.get("os") or "").lower()
    device_id = env.get("idfa", "") if os_ == "ios" else env.get("gaid", "")
    p_host, p_port, p_user, p_pass = _proxy_parts(env.get("proxy", ""))
    p_scheme = _proxy_scheme(env.get("proxy", ""))
    events = json.dumps([{"name": e} for e in data["events"]])
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO scheduled_jobs
                         (user_id, name, events, package, dev_key, gaid, afid, os,
                          proxy_host, proxy_port, proxy_user, proxy_pass, proxy_scheme, run_at, enabled)
                       VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW(), 1)
                       RETURNING id""",
                    (user["id"], data["name"], events, data["package"], data["dev_key"],
                     device_id, env.get("afid", ""), os_, p_host, p_port, p_user, p_pass, p_scheme),
                )
                return cur.fetchone()["id"]
    except Exception as e:
        logger.error(f"[Telegram] /add save failed: {e}")
        return None


# ── دوال مشتركة للقائمة الرئيسية (يستخدمها الأمر والزر معاً) ──────────────────
def _settings_kb(uid):
    on = _get_notify(uid)
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton(
        _t("settings_notify_on" if on else "settings_notify_off", uid),
        callback_data="settings:toggle",
    ))
    return kb


def _open_apps(chat_id, user):
    if not GAMES_DATA:
        bot.send_message(chat_id, _t("no_apps_defined", user["id"]))
        return
    _set_state(user["id"], "apps_os", {})   # entry of the cascading flow (no history)
    _render_os_ui(chat_id, user)


def _open_profile(chat_id, user):
    uid = user["id"]
    env = _get_env(uid)
    os_ = env.get("os", "") or "—"
    dev_id = env.get("idfa", "") if os_ == "ios" else env.get("gaid", "")
    dev_lbl = "IDFA" if os_ == "ios" else "GAID"
    txt = _t("profile_box", uid,
             user=html.escape(str(user.get("username", ""))),
             os=html.escape(str(os_)),
             devlbl=dev_lbl,
             devid=html.escape(str(dev_id or "—")),
             afid=html.escape(str(env.get("afid", "") or "—")),
             proxy=html.escape(str(env.get("proxy", "") or "—")))
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(_t("btn_upd_device", uid), callback_data="profile:device"),
        types.InlineKeyboardButton(_t("btn_upd_proxy", uid), callback_data="profile:proxy"),
    )
    kb.row(types.InlineKeyboardButton(_t("btn_diag_toggle", uid), callback_data="profile:diag"))
    kb.row(_home_btn(uid))
    bot.send_message(chat_id, txt, parse_mode="HTML", reply_markup=kb)


_MENU_KEYS = [
    ("btn_new_task", "add"),
    ("btn_apps",     "apps"),
    ("btn_profile",  "profile"),
    ("btn_settings", "settings"),
    ("btn_balance",  "balance"),
    ("btn_services", "services"),
    ("btn_lang",     "language"),
]


def _main_menu_kb(uid):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(_t("btn_new_task", uid), _t("btn_apps", uid))
    kb.row(_t("btn_profile", uid), _t("btn_settings", uid))
    kb.row(_t("btn_balance", uid), _t("btn_services", uid))
    kb.row(_t("btn_lang", uid))
    return kb


def _send_lang_menu(chat_id, uid):
    """Language picker — native names, one button per supported language."""
    kb = types.InlineKeyboardMarkup()
    row = [types.InlineKeyboardButton(locales.name(code), callback_data=f"lang:set:{code}")
           for code in locales.SUPPORTED]
    kb.row(*row)
    bot.send_message(chat_id, _t("lang_choose", uid), reply_markup=kb)


def _menu_target(text):
    """يطابق نص الزر بأي لغة مدعومة مع هدفه (يدعم تبديل اللغة)."""
    for lang in locales.SUPPORTED:
        for key, target in _MENU_KEYS:
            if locales.lookup(key, lang) == text:
                return target
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# مركز الخدمات + المهام (عرض)
# ═══════════════════════════════════════════════════════════════════════════════
def _open_services(chat_id, u):
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton(_t("btn_subscribe", u["id"]),  callback_data="svc:plans"))
    kb.row(types.InlineKeyboardButton(_t("btn_sub_status", u["id"]), callback_data="svc:status"))
    kb.row(types.InlineKeyboardButton(_t("btn_support", u["id"]),    callback_data="svc:support"))
    kb.row(_home_btn(u["id"]))
    bot.send_message(chat_id, _t("services_menu", u["id"]), reply_markup=kb)


def _send_plans(chat_id, u):
    kb = types.InlineKeyboardMarkup()
    for key, p in _get_plans().items():
        price = f" · {p['price']}" if p.get("price") else ""
        label = f"{_plan_label(key, u['id'])} · {p['credits']}/{p['days']}d{price}"
        kb.row(types.InlineKeyboardButton(label, callback_data=f"pay:plan:{key}"))
    kb.row(types.InlineKeyboardButton(_t("btn_cancel", u["id"]), callback_data="svc:close"))
    kb.row(_home_btn(u["id"]))
    bot.send_message(chat_id, _t("choose_plan", u["id"]), reply_markup=kb)


def _send_sub_status(chat_id, u):
    expiry = _sub_expiry(u["id"])
    fresh = _user_by_chat(chat_id) or u
    kb = types.InlineKeyboardMarkup()
    kb.row(_home_btn(u["id"]))
    if expiry:
        until = expiry.strftime("%Y-%m-%d") if hasattr(expiry, "strftime") else str(expiry)
        bot.send_message(chat_id, _t("sub_active", u["id"], until=until, credits=fresh["uses_left"]),
                         parse_mode="HTML", reply_markup=kb)
    else:
        bot.send_message(chat_id, _t("sub_none", u["id"], credits=fresh["uses_left"]), reply_markup=kb)


def _user_jobs(user_id, limit=20):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, enabled FROM scheduled_jobs WHERE user_id=%s ORDER BY id DESC LIMIT %s",
                (user_id, limit),
            )
            return cur.fetchall()


def _job_owned(job_id, user):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM scheduled_jobs WHERE id=%s", (job_id,))
            job = cur.fetchone()
    if not job or (job["user_id"] != user["id"] and user["role"] != "admin"):
        return None
    return job


def _open_jobs(chat_id, u):
    jobs = _user_jobs(u["id"])
    kb = types.InlineKeyboardMarkup()
    if not jobs:
        kb.row(_home_btn(u["id"]))
        bot.send_message(chat_id, _t("jobs_empty", u["id"]), reply_markup=kb)
        return
    for j in jobs:
        mark = "✅" if j["enabled"] else "⏸️"
        kb.row(types.InlineKeyboardButton(f"{mark} {j['name']}", callback_data=f"job:view:{j['id']}"))
    kb.row(_home_btn(u["id"]))
    bot.send_message(chat_id, _t("jobs_title", u["id"]), reply_markup=kb)


def _send_job_detail(chat_id, u, job):
    try:
        evs = ", ".join(e.get("name", "") for e in (job.get("events") or []))
    except Exception:
        evs = str(job.get("events"))
    status = _t("job_on" if job["enabled"] else "job_off", u["id"])
    txt = _t("job_detail", u["id"],
             name=html.escape(job["name"]),
             package=html.escape(job.get("package", "")),
             events=html.escape(evs),
             status=status)
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(_t("btn_run", u["id"]),    callback_data=f"run:{job['id']}"),
        types.InlineKeyboardButton(_t("btn_toggle", u["id"]), callback_data=f"tog:{job['id']}"),
    )
    kb.row(
        types.InlineKeyboardButton(_t("btn_edit", u["id"]),   callback_data=f"job:edit:{job['id']}"),
        types.InlineKeyboardButton(_t("btn_delete", u["id"]), callback_data=f"del:{job['id']}"),
    )
    bot.send_message(chat_id, txt, parse_mode="HTML", reply_markup=kb)


_EDIT_FIELDS = {"name": "btn_f_name", "dev_key": "btn_f_devkey", "events": "btn_f_events"}


def _send_edit_menu(chat_id, u, job_id):
    kb = types.InlineKeyboardMarkup()
    for field, lbl in _EDIT_FIELDS.items():
        kb.row(types.InlineKeyboardButton(_t(lbl, u["id"]), callback_data=f"edit:{field}:{job_id}"))
    bot.send_message(chat_id, _t("edit_menu", u["id"]), reply_markup=kb)


def _apply_job_edit(job, field, value):
    """يحدّث حقلاً واحداً في scheduled_jobs (قائمة بيضاء للأعمدة). يعيد True/False."""
    import json as _json
    if field not in _EDIT_FIELDS:
        return False
    if field == "events":
        evs = [e.strip() for e in value.split(",") if e.strip()]
        if not evs:
            return False
        new_val = _json.dumps([{"name": e} for e in evs])
        col_expr = "events = %s::jsonb"
    elif field == "name":
        if not value.strip():
            return False
        new_val = value.strip()
        col_expr = "name = %s"
    else:  # dev_key
        if len(value.strip()) < 6:
            return False
        new_val = value.strip()
        col_expr = "dev_key = %s"
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE scheduled_jobs SET {col_expr} WHERE id=%s", (new_val, job["id"]))
        return True
    except Exception as e:
        logger.error(f"[Telegram] job edit failed: {e}")
        return False


def _register_handlers():
    if not bot:
        return

    # ── /start — إعادة ضبط شاملة (Factory Reset) + القائمة الرئيسية ──────────
    @bot.message_handler(commands=["start"])
    def h_start(m):
        parts = (m.text or "").split(maxsplit=1)
        u = _user_by_chat(m.chat.id)
        if not u and len(parts) == 2:
            uid = _consume_link_code(parts[1].strip().upper())
            if uid:
                _set_chat(uid, m.chat.id)
                u = _user_by_chat(m.chat.id)
                _clear_state(u["id"])
                bot.reply_to(m, _t("linked_ok", u["id"], name=html.escape(u["username"])))
                return
        if u:
            _clear_state(u["id"])   # إعادة ضبط: مسح كل الحالات المؤقتة
            bot.send_message(
                m.chat.id,
                _t("welcome_back", u["id"], name=u["username"], credits=u["uses_left"], max=u["max_uses"]),
                reply_markup=_main_menu_kb(u["id"]),
            )
            return
        u = _auto_register(m.chat.id, m.from_user.username, m.from_user.first_name)
        _clear_state(u["id"])
        bot.send_message(
            m.chat.id,
            _t("welcome", u["id"], name=u["username"], credits=u["uses_left"]),
            reply_markup=_main_menu_kb(u["id"]),
        )

    # ── /lang — تبديل اللغة ──────────────────────────────────────────────────
    @bot.message_handler(commands=["lang"])
    @linked
    def h_lang(m, u):
        _send_lang_menu(m.chat.id, u["id"])

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("lang:set:"))
    def h_lang_cb(c):
        bot.answer_callback_query(c.id)                       # dismiss spinner first
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        code = c.data.rsplit(":", 1)[1]
        if code in locales.SUPPORTED:
            _set_lang(u["id"], code)
        bot.send_message(c.message.chat.id, _t("lang_set", u["id"]), reply_markup=_main_menu_kb(u["id"]))

    # ── /services — مركز الخدمات (المطلب 7) ──────────────────────────────────
    @bot.message_handler(commands=["services"])
    @linked
    def h_services(m, u):
        _open_services(m.chat.id, u)

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("svc:"))
    def h_svc_cb(c):
        bot.answer_callback_query(c.id)                       # dismiss spinner first
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        action = c.data.split(":", 1)[1]
        if action == "plans":
            _send_plans(c.message.chat.id, u)
        elif action == "status":
            _send_sub_status(c.message.chat.id, u)
        elif action == "support":
            _set_state(u["id"], "support_msg", {})
            bot.send_message(c.message.chat.id, _t("support_prompt", u["id"]),
                             reply_markup=types.ForceReply(selective=False))
        elif action == "close":
            _clear_state(u["id"])
            bot.send_message(c.message.chat.id, _t("reset_done", u["id"]), reply_markup=_main_menu_kb(u["id"]))

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("pay:"))
    def h_pay_cb(c):
        bot.answer_callback_query(c.id)                       # absolute first: clear spinner
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        parts = c.data.split(":")
        action = parts[1] if len(parts) > 1 else ""
        # plan chosen → instructions (with wallet) + wait for the screenshot
        if action == "plan" and len(parts) > 2:
            plan_key = parts[2]
            p = _get_plan(plan_key)
            if not p:
                return
            _set_state(u["id"], "pay_screenshot", {"plan": plan_key})
            wallet = _get_setting("wallet_address", "—")
            bot.send_message(
                c.message.chat.id,
                _t("pay_instructions", u["id"], plan=_plan_label(plan_key, u["id"]),
                   credits=p["credits"], days=p["days"], wallet=html.escape(str(wallet))),
                parse_mode="HTML",
            )
            return
        # approve / reject (admins only)
        if action in ("approve", "reject") and len(parts) > 2 and u["role"] == "admin":
            pid = int(parts[2]) if parts[2].isdigit() else 0
            row = _set_payment_status(pid, "approved" if action == "approve" else "rejected")
            if not row:
                bot.send_message(c.message.chat.id, _t("pay_not_found", u["id"]))
                return
            target = _user_by_id(row["user_id"])
            if action == "approve":
                expires = _grant_subscription(row["user_id"], row["plan"])
                until = expires.strftime("%Y-%m-%d") if hasattr(expires, "strftime") else str(expires)
                p = _get_plan(row["plan"]) or {}
                if target and target.get("tg_chat_id"):
                    bot.send_message(int(target["tg_chat_id"]),
                                     _t("pay_approved", target["id"], plan=_plan_label(row["plan"], target["id"]),
                                        until=until, credits=p.get("credits", 0)), parse_mode="HTML")
                bot.send_message(c.message.chat.id, _t("pay_approved_admin", u["id"], uid=row["user_id"], until=until))
            else:
                if target and target.get("tg_chat_id"):
                    bot.send_message(int(target["tg_chat_id"]), _t("pay_rejected", target["id"]))
                bot.send_message(c.message.chat.id, _t("pay_rejected_admin", u["id"], id=pid))
            return

    # استقبال لقطة الدفع (صورة) أثناء حالة pay_screenshot
    @bot.message_handler(content_types=["photo"])
    def h_payment_photo(m):
        u = _user_by_chat(m.chat.id)
        if not u:
            return
        step, data = _get_state(u["id"])
        if step != "pay_screenshot":
            return
        plan_key = data.get("plan")
        file_id = m.photo[-1].file_id
        _clear_state(u["id"])
        pid = _create_payment(u["id"], plan_key, file_id)
        if pid:
            # Saved silently as 'pending' — admins review it from the /admin dashboard
            # (no direct push to admin chats anymore).
            bot.reply_to(m, _t("pay_received", u["id"]))
        else:
            bot.reply_to(m, _t("pay_record_fail", u["id"]))

    # ── /payments — مراجعة المشرف ────────────────────────────────────────────
    @bot.message_handler(commands=["payments"])
    @linked
    def h_payments(m, u):
        if u["role"] != "admin":
            bot.reply_to(m, _t("admins_only", u["id"]))
            return
        pend = _list_pending_payments()
        if not pend:
            bot.reply_to(m, _t("no_pending_pay", u["id"]))
            return
        for pay in pend:
            target = _user_by_id(pay["user_id"])
            cap = _t("admin_pay_request", u["id"], id=pay["id"],
                     user=(target["username"] if target else pay["user_id"]), plan=_plan_label(pay["plan"]))
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton(_t("btn_approve", u["id"]), callback_data=f"pay:approve:{pay['id']}"),
                types.InlineKeyboardButton(_t("btn_reject", u["id"]),  callback_data=f"pay:reject:{pay['id']}"),
            )
            try:
                bot.send_photo(m.chat.id, pay["screenshot"], caption=cap, reply_markup=kb)
            except Exception:
                bot.send_message(m.chat.id, cap, reply_markup=kb)

    # ── /admin — dashboard (replaces direct push of payments/support) ────────
    @bot.message_handler(commands=["admin"])
    @linked
    def h_admin(m, u):
        if u["role"] != "admin":
            bot.reply_to(m, _t("admins_only", u["id"]))
            return
        _open_admin(m.chat.id, u)

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("adm:"))
    def h_admin_cb(c):
        bot.answer_callback_query(c.id)                       # instant ack
        u = _user_by_chat(c.message.chat.id)
        if not u or u["role"] != "admin":
            return
        parts = (c.data or "").split(":")
        action = parts[1] if len(parts) > 1 else ""
        cid = c.message.chat.id
        if action == "home":
            _open_admin(cid, u)
        elif action == "pay":
            _admin_next_payment(cid, u)
        elif action == "sup":
            _admin_next_ticket(cid, u)
        elif action == "users":
            off = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            _admin_show_users(cid, u, off)
        elif action == "user" and len(parts) > 2:
            _admin_show_user(cid, u, int(parts[2]))
        elif action == "ucredit" and len(parts) > 2:
            _set_state(u["id"], "admin_user_credit", {"uid": int(parts[2])})
            bot.send_message(cid, _t("admin_ask_credits", u["id"]),
                             reply_markup=types.ForceReply(selective=False))
        elif action == "uplan" and len(parts) > 2:
            _admin_grant_menu(cid, u, int(parts[2]))
        elif action == "ugrant" and len(parts) > 3:
            target_uid, key = int(parts[2]), parts[3]
            expires = _grant_subscription(target_uid, key)
            until = expires.strftime("%Y-%m-%d") if hasattr(expires, "strftime") else str(expires)
            tgt = _user_by_id(target_uid)
            if tgt and tgt.get("tg_chat_id"):
                p = _get_plan(key) or {}
                try:
                    bot.send_message(int(tgt["tg_chat_id"]),
                                     _t("pay_approved", target_uid, plan=_plan_label(key, target_uid),
                                        until=until, credits=p.get("credits", 0)), parse_mode="HTML")
                except Exception:
                    pass
            bot.send_message(cid, _t("admin_grant_done", u["id"], until=until))
            _admin_show_user(cid, u, target_uid)
        elif action == "udel" and len(parts) > 2:
            target_uid = int(parts[2])
            kb = types.InlineKeyboardMarkup()
            kb.row(types.InlineKeyboardButton(_t("btn_confirm_del", u["id"]), callback_data=f"adm:udelok:{target_uid}"))
            kb.row(types.InlineKeyboardButton(_t("btn_back", u["id"]), callback_data=f"adm:user:{target_uid}"))
            bot.send_message(cid, _t("admin_user_view", u["id"],
                                     username="?", uid=target_uid, role="", left="", max=""), reply_markup=kb)
        elif action == "udelok" and len(parts) > 2:
            target_uid = int(parts[2])
            if target_uid != u["id"]:
                _delete_user(target_uid)
            bot.send_message(cid, _t("admin_user_deleted", u["id"]))
            _admin_show_users(cid, u, 0)
        elif action == "plans":
            _admin_show_plans(cid, u)
        elif action == "plan" and len(parts) > 2:
            _admin_show_plan(cid, u, parts[2])
        elif action == "ptoggle" and len(parts) > 2:
            p = _get_plan(parts[2])
            if p:
                _update_plan(parts[2], active=0 if p.get("active", 1) else 1)
            _admin_show_plan(cid, u, parts[2])
        elif action == "pedit" and len(parts) > 3:
            key, field = parts[2], parts[3]
            _set_state(u["id"], "admin_plan_edit", {"key": key, "field": field})
            bot.send_message(cid, _t("admin_ask_value", u["id"], field=field),
                             parse_mode="HTML", reply_markup=types.ForceReply(selective=False))
        elif action == "pdel" and len(parts) > 2:
            _delete_plan(parts[2])
            bot.send_message(cid, _t("plan_deleted", u["id"]))
            _admin_show_plans(cid, u)
        elif action == "padd":
            # guided wizard: key → en → ar → bn → credits → days → price
            _set_state(u["id"], "pw_key", {})
            bot.send_message(cid, _t("pw_ask_key", u["id"]), parse_mode="HTML",
                             reply_markup=types.ForceReply(selective=False))
        elif action == "payset":
            _admin_show_payset(cid, u)
        elif action == "setwallet":
            _set_state(u["id"], "admin_wallet", {})
            bot.send_message(cid, _t("admin_ask_wallet", u["id"]),
                             reply_markup=types.ForceReply(selective=False))
        elif action == "bc":
            _set_state(u["id"], "admin_broadcast", {})
            bot.send_message(cid, _t("admin_ask_broadcast", u["id"]), parse_mode="HTML",
                             reply_markup=types.ForceReply(selective=False))

    # ── Universal "🏠 Main menu" — clears any state, never a dead end ────────
    @bot.callback_query_handler(func=lambda c: c.data == "home")
    def h_home_cb(c):
        bot.answer_callback_query(c.id)
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        _clear_state(u["id"])
        bot.send_message(c.message.chat.id, _t("main_menu", u["id"]), reply_markup=_main_menu_kb(u["id"]))


    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("job:"))
    def h_job_view_cb(c):
        bot.answer_callback_query(c.id)                       # dismiss spinner first
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        parts = c.data.split(":")
        action = parts[1] if len(parts) > 1 else ""
        jid = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        job = _job_owned(jid, u)
        if not job:
            bot.send_message(c.message.chat.id, _t("job_not_found", u["id"]))
            return
        if action == "view":
            _send_job_detail(c.message.chat.id, u, job)
        elif action == "edit":
            _send_edit_menu(c.message.chat.id, u, jid)

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("edit:"))
    def h_edit_cb(c):
        bot.answer_callback_query(c.id)                       # dismiss spinner first
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        parts = c.data.split(":")
        if len(parts) < 3:
            return
        field, jid = parts[1], (int(parts[2]) if parts[2].isdigit() else 0)
        job = _job_owned(jid, u)
        if not job or field not in _EDIT_FIELDS:
            return
        # القيمة الحالية (معبّأة مسبقاً)
        if field == "events":
            try:
                cur = ", ".join(e.get("name", "") for e in (job.get("events") or []))
            except Exception:
                cur = ""
        else:
            cur = str(job.get(field, ""))
        _set_state(u["id"], "edit_value", {"job_id": jid, "field": field})
        bot.send_message(c.message.chat.id, _t("edit_prompt", u["id"], cur=html.escape(cur or "—")),
                         parse_mode="HTML", reply_markup=types.ForceReply(selective=False))

    @bot.message_handler(commands=["help"])
    def h_help(m):
        u = _user_by_chat(m.chat.id)
        bot.reply_to(m, _t("help_text", u["id"] if u else 0))

    @bot.message_handler(commands=["unlink"])
    def h_unlink(m):
        u = _user_by_chat(m.chat.id)
        _uid = u["id"] if u else 0
        _clear_chat(m.chat.id)
        bot.reply_to(m, _t("unlink_ok", _uid))

    @bot.message_handler(commands=["balance"])
    @linked
    def h_balance(m, u):
        bot.reply_to(m, _t("balance_line", u["id"], left=u["uses_left"], max=u["max_uses"]))

    @bot.message_handler(commands=["status"])
    @linked
    def h_status(m, u):
        bot.reply_to(m, _t("status_line", u["id"], username=html.escape(u["username"]), role=u["role"], left=u["uses_left"], max=u["max_uses"]))

    @bot.message_handler(commands=["history"])
    @linked
    def h_history(m, u):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT event_name, status, ok, created_at FROM event_history
                       WHERE user_id=%s ORDER BY id DESC LIMIT 8""", (u["id"],))
                rows = cur.fetchall()
        if not rows:
            bot.reply_to(m, _t("history_empty", u["id"]))
            return
        lines = []
        for r in rows:
            mark = "✅" if r["ok"] else "❌"
            ts = r["created_at"].strftime("%m-%d %H:%M") if r["created_at"] else ""
            lines.append(f"{mark} {r['event_name']} → {r['status']}  {ts}")
        bot.reply_to(m, _t("history_title", u["id"]) + "\n" + "\n".join(lines))

    # ── /profile ─────────────────────────────────────────────────────────────
    @bot.message_handler(commands=["profile"])
    @linked
    def h_profile(m, u):
        _open_profile(m.chat.id, u)

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("profile:"))
    def h_profile_cb(c):
        bot.answer_callback_query(c.id)                       # dismiss spinner first
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        action = c.data.split(":", 1)[1]
        if action == "device":
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("🤖 Android", callback_data="os:android"),
                types.InlineKeyboardButton("🍎 iOS", callback_data="os:ios"),
            )
            bot.send_message(c.message.chat.id, _t("choose_os", u["id"]), reply_markup=kb)
        elif action == "proxy":
            # Start the multi-step proxy wizard with a scheme picker.
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("HTTP", callback_data="pxs:http"),
                types.InlineKeyboardButton("SOCKS5", callback_data="pxs:socks5"),
            )
            kb.row(types.InlineKeyboardButton(_t("btn_cancel", u["id"]), callback_data="home"))
            bot.send_message(c.message.chat.id, _t("proxy_scheme", u["id"]),
                             parse_mode="HTML", reply_markup=kb)
        elif action == "diag":
            new = not _get_diag(u["id"])
            _set_diag(u["id"], new)
            bot.send_message(c.message.chat.id, _t("diag_on" if new else "diag_off", u["id"]),
                             parse_mode="HTML")

    # ── Proxy wizard callbacks: scheme picker + auth yes/no ──────────────────
    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("pxs:"))
    def h_proxy_scheme_cb(c):
        bot.answer_callback_query(c.id)
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        scheme = c.data.split(":", 1)[1]
        if scheme not in ("http", "socks5"):
            return
        _set_state(u["id"], "proxy_host", {"scheme": scheme})
        bot.send_message(c.message.chat.id, _t("proxy_ask_host", u["id"]),
                         parse_mode="HTML", reply_markup=types.ForceReply(selective=False))

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("pxauth:"))
    def h_proxy_auth_cb(c):
        bot.answer_callback_query(c.id)
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        step, data = _get_state(u["id"])
        choice = c.data.split(":", 1)[1]
        if choice == "yes":
            _set_state(u["id"], "proxy_user", data)
            bot.send_message(c.message.chat.id, _t("proxy_ask_user", u["id"]),
                             parse_mode="HTML", reply_markup=types.ForceReply(selective=False))
        else:
            _finish_proxy(c.message.chat.id, u, data)

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("os:"))
    def h_os_cb(c):
        bot.answer_callback_query(c.id)                       # dismiss spinner first
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        os_ = c.data.split(":", 1)[1]
        if os_ == "android":
            _set_state(u["id"], "device_gaid", {"os": "android"})
            bot.send_message(c.message.chat.id, _t("ask_gaid", u["id"]), parse_mode="HTML", reply_markup=types.ForceReply(selective=False))
        else:
            _set_state(u["id"], "device_idfa", {"os": "ios"})
            bot.send_message(c.message.chat.id, _t("ask_idfa", u["id"]), parse_mode="HTML", reply_markup=types.ForceReply(selective=False))

    # ── /settings ────────────────────────────────────────────────────────────
    @bot.message_handler(commands=["settings"])
    @linked
    def h_settings(m, u):
        bot.send_message(m.chat.id, _t("settings_title", u["id"]), reply_markup=_settings_kb(u["id"]))

    @bot.callback_query_handler(func=lambda c: c.data == "settings:toggle")
    def h_settings_cb(c):
        bot.answer_callback_query(c.id)                       # absolute first: clear spinner
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        _set_notify(u["id"], not _get_notify(u["id"]))
        try:
            bot.edit_message_reply_markup(c.message.chat.id, c.message.message_id, reply_markup=_settings_kb(u["id"]))
        except Exception:
            pass

    # ── /apps ────────────────────────────────────────────────────────────────
    @bot.message_handler(commands=["apps"])
    @linked
    def h_apps(m, u):
        _open_apps(m.chat.id, u)

    # ── /add — إنشاء مهمة مجدولة بخطوات ──────────────────────────────────────
    @bot.message_handler(commands=["add"])
    @linked
    def h_add(m, u):
        _set_state(u["id"], "add_name", {})
        _send_add_step(m.chat.id, "add_name", {}, uid=u["id"])

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("add:"))
    def h_add_cb(c):
        bot.answer_callback_query(c.id)                       # dismiss spinner first
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        action = c.data.split(":", 1)[1]
        if action == "cancel":
            _clear_state(u["id"])
            bot.send_message(c.message.chat.id, _t("add_cancelled", u["id"]))
            return
        if action == "restart":
            _set_state(u["id"], "add_name", {})
            _send_add_step(c.message.chat.id, "add_name", {}, uid=u["id"])
            return
        if action == "back":
            step, data = _get_state(u["id"])
            prev = _ADD_PREV.get(step)
            if prev:
                _set_state(u["id"], prev, data)
                _send_add_step(c.message.chat.id, prev, data, uid=u["id"])
            return
        if action == "save":
            step, data = _get_state(u["id"])
            if step != "awaiting_confirm":
                bot.send_message(c.message.chat.id, _t("session_over", u["id"]))
                return
            if not all(data.get(k) for k in ("name", "package", "dev_key", "events")):
                _clear_state(u["id"])
                bot.send_message(c.message.chat.id, _t("add_incomplete", u["id"]))
                return
            jid = _save_add_job(u, data)
            _clear_state(u["id"])
            if jid:
                env = _get_env(u["id"])
                ready = (env.get("os") and (env.get("gaid") or env.get("idfa")) and env.get("afid"))
                note = "" if ready else _t("add_saved_note", u["id"])
                bot.send_message(c.message.chat.id, _t("add_saved", u["id"], id=jid, note=note), parse_mode="HTML")
            else:
                bot.send_message(c.message.chat.id, _t("add_save_fail", u["id"]))
            return
        if action.startswith("exv:"):
            value = action[len("exv:"):]
            _add_advance(c.message.chat.id, u, value)
            return

    # ── القائمة الرئيسية (ReplyKeyboard) — تربط الأزرار بالأوامر (i18n) ───────
    @bot.message_handler(
        func=lambda m: bool(m.text) and _menu_target(m.text) is not None and _text_step(m.chat.id) is None,
        content_types=["text"],
    )
    def h_menu(m):
        u = _user_by_chat(m.chat.id)
        if not u:
            bot.reply_to(m, locales.lookup("need_start"))
            return
        target = _menu_target(m.text)
        if target == "add":
            _set_state(u["id"], "add_name", {})
            _send_add_step(m.chat.id, "add_name", {}, uid=u["id"])
        elif target == "apps":
            _open_apps(m.chat.id, u)
        elif target == "profile":
            _open_profile(m.chat.id, u)
        elif target == "settings":
            bot.send_message(m.chat.id, _t("settings_title", u["id"]), reply_markup=_settings_kb(u["id"]))
        elif target == "balance":
            bot.send_message(m.chat.id, _t("balance_line", u["id"], left=u["uses_left"], max=u["max_uses"]))
        elif target == "services":
            _open_services(m.chat.id, u)
        elif target == "language":
            _send_lang_menu(m.chat.id, u["id"])

    # ── التدفّق المتسلسل: OS → فئة → تطبيق → جمع بيانات الجهاز ────────────────
    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("nav:"))
    def h_nav_cb(c):
        bot.answer_callback_query(c.id)                       # absolute first: clear spinner
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        parts = (c.data or "").split(":")
        action = parts[1] if len(parts) > 1 else ""
        chat = c.message.chat.id

        if action == "cancel":
            _clear_state(u["id"])
            bot.send_message(chat, _t("reset_done", u["id"]), reply_markup=_main_menu_kb(u["id"]))
            return

        if action == "back":
            step, data = _nav_back(u["id"])
            if step is None:
                _set_state(u["id"], "apps_os", {})
                _render_os_ui(chat, u)
            else:
                _render_step(chat, u, step, data)
            return

        step, data = _get_state(u["id"])
        try:
            if action == "os" and len(parts) > 2:
                _nav_push(u["id"], "apps_cat", {"os": parts[2]})
                _render_categories_ui(chat, u, parts[2])

            elif action == "cat" and len(parts) > 2:
                os_ = data.get("os")
                cats = _cat_list(os_)
                ci = int(parts[2])
                if 0 <= ci < len(cats):
                    _nav_push(u["id"], "apps_game", {"os": os_, "cat": cats[ci]})
                    _render_games_ui(chat, u, os_, cats[ci])

            elif action == "game" and len(parts) > 2:
                ai = int(parts[2])
                if 0 <= ai < len(GAMES_DATA):
                    os_ = data.get("os") or GAMES_DATA[ai].get("os", "android")
                    nxt = "task_idfa" if os_ == "ios" else "task_gaid"
                    _nav_push(u["id"], nxt, {"os": os_, "cat": data.get("cat"), "app_index": ai})
                    _ask_device(chat, u, nxt)
        except (ValueError, IndexError):
            _clear_state(u["id"])
            bot.send_message(chat, _t("session_over", u["id"]))

    # ── اختيار الوضع: 🎯 القناص (فوري) / ✍️ المخصّص (جدولة) ───────────────────
    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("mode:"))
    def h_mode_cb(c):
        bot.answer_callback_query(c.id)                       # absolute first: clear spinner
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        action = c.data.split(":", 1)[1]
        if action == "cancel":
            _clear_state(u["id"])
            bot.send_message(c.message.chat.id, _t("reset_done", u["id"]), reply_markup=_main_menu_kb(u["id"]))
            return
        step, data = _get_state(u["id"])
        if step != "awaiting_mode":
            bot.send_message(c.message.chat.id, _t("session_over", u["id"]))
            return
        if action == "sniper":
            _set_state(u["id"], "sniper_value", data)
            bot.send_message(c.message.chat.id, _t("ask_event", u["id"]),
                             parse_mode="HTML", reply_markup=types.ForceReply(selective=False))
        elif action == "custom":
            _set_state(u["id"], "custom_input", data)
            bot.send_message(c.message.chat.id, _t("ask_delay", u["id"]),
                             parse_mode="HTML", reply_markup=types.ForceReply(selective=False))

    # ── أزرار المهام القديمة ──────────────────────────────────────────────────
    @bot.callback_query_handler(func=lambda c: (c.data or "").split(":")[0] in ("run", "tog", "del"))
    def h_job_cb(c):
        bot.answer_callback_query(c.id)                       # absolute first: clear spinner
        u = _user_by_chat(c.message.chat.id)
        if not u:
            return
        action, _, sid = (c.data or "").partition(":")
        if not sid.isdigit():
            return
        jid = int(sid)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM scheduled_jobs WHERE id=%s", (jid,))
                job = cur.fetchone()
        if not job or (job["user_id"] != u["id"] and u["role"] != "admin"):
            bot.send_message(c.message.chat.id, _t("not_allowed", u["id"]))
            return
        if action == "run":
            execute_job.apply_async(args=[jid], countdown=0)
            bot.send_message(c.message.chat.id, _t("job_run_toast", u["id"]))
        elif action == "tog":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE scheduled_jobs SET enabled = 1 - enabled WHERE id=%s", (jid,))
            bot.send_message(c.message.chat.id, _t("job_toggled", u["id"]))
        elif action == "del":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM scheduled_jobs WHERE id=%s", (jid,))
            bot.send_message(c.message.chat.id, _t("job_deleted", u["id"]))

    # ── معالج النص (آلة الحالة) — محميّ بالكامل ───────────────────────────────
    def _text_step(chat_id):
        u = _user_by_chat(chat_id)
        if not u:
            return None
        step, _ = _get_state(u["id"])
        return step if step in TEXT_STEPS else None

    @bot.message_handler(
        func=lambda m: bool(m.text) and not m.text.startswith("/") and _text_step(m.chat.id) is not None,
        content_types=["text"],
    )
    def h_text(m):
        u = _user_by_chat(m.chat.id)
        if not u:
            return
        step, data = _get_state(u["id"])
        text = (m.text or "").strip()
        try:
            # مسار /add متعدّد الخطوات (تحقّق ذاتي داخل _add_advance)
            if step in ("add_name", "add_package", "add_devkey", "add_events"):
                _add_advance(m.chat.id, u, text)
                return

            # رفض الإدخال الفارغ للخطوات التي تتطلّب قيمة (نُبقي الحالة لإعادة المحاولة)
            if step in ("device_gaid", "device_idfa", "device_afid", "sniper_value") and not text:
                bot.reply_to(m, _t("empty_value", u["id"]),
                             reply_markup=types.ForceReply(selective=False))
                return

            if step == "device_gaid":
                data["gaid"] = text
                _set_state(u["id"], "device_afid", data)
                bot.reply_to(m, _t("ask_afid", u["id"]), parse_mode="HTML", reply_markup=types.ForceReply(selective=False))

            elif step == "device_idfa":
                data["idfa"] = text
                _set_state(u["id"], "device_afid", data)
                bot.reply_to(m, _t("ask_afid", u["id"]), parse_mode="HTML", reply_markup=types.ForceReply(selective=False))

            elif step == "device_afid":
                _save_env(u["id"], os=data.get("os"), gaid=data.get("gaid", ""),
                          idfa=data.get("idfa", ""), afid=text)
                _clear_state(u["id"])
                bot.reply_to(m, _t("device_saved", u["id"]))

            elif step == "proxy_host":
                if not text:
                    bot.reply_to(m, _t("proxy_ask_host", u["id"]), parse_mode="HTML",
                                 reply_markup=types.ForceReply(selective=False))
                    return
                data["host"] = text
                _set_state(u["id"], "proxy_port", data)
                bot.reply_to(m, _t("proxy_ask_port", u["id"]), parse_mode="HTML",
                             reply_markup=types.ForceReply(selective=False))

            elif step == "proxy_port":
                if not text.isdigit():
                    bot.reply_to(m, _t("proxy_ask_port", u["id"]), parse_mode="HTML",
                                 reply_markup=types.ForceReply(selective=False))
                    return
                data["port"] = text
                _set_state(u["id"], "proxy_auth", data)   # holding state; resolved by pxauth callback
                kb = types.InlineKeyboardMarkup()
                kb.row(
                    types.InlineKeyboardButton(_t("btn_yes", u["id"]), callback_data="pxauth:yes"),
                    types.InlineKeyboardButton(_t("btn_no", u["id"]),  callback_data="pxauth:no"),
                )
                bot.reply_to(m, _t("proxy_ask_auth", u["id"]), parse_mode="HTML", reply_markup=kb)

            elif step == "proxy_user":
                if not text:
                    bot.reply_to(m, _t("proxy_ask_user", u["id"]), parse_mode="HTML",
                                 reply_markup=types.ForceReply(selective=False))
                    return
                data["user"] = text
                _set_state(u["id"], "proxy_pass", data)
                bot.reply_to(m, _t("proxy_ask_pass", u["id"]), parse_mode="HTML",
                             reply_markup=types.ForceReply(selective=False))

            elif step == "proxy_pass":
                data["pass"] = text
                _finish_proxy(m.chat.id, u, data)

            # ── جمع بيانات الجهاز لكل مهمة (عزل البيانات) ────────────────────
            elif step in ("task_gaid", "task_idfa"):
                if not text:
                    _ask_device(m.chat.id, u, step)
                    return
                field = "gaid" if step == "task_gaid" else "idfa"
                fwd = {k: v for k, v in data.items() if k != "__hist"}
                fwd[field] = text
                _nav_push(u["id"], "task_afid", fwd)
                _ask_device(m.chat.id, u, "task_afid")

            elif step == "task_afid":
                if not text:
                    _ask_device(m.chat.id, u, step)
                    return
                fwd = {k: v for k, v in data.items() if k != "__hist"}
                fwd["afid"] = text
                _nav_push(u["id"], "awaiting_mode", fwd)
                idx = fwd.get("app_index", -1)
                if 0 <= idx < len(GAMES_DATA):
                    _send_mode_menu(m.chat.id, GAMES_DATA[idx], u["id"])

            elif step == "sniper_value":
                # 🎯 القناص: تنفيذ فوري ببيانات الجهاز الخاصة بالمهمة
                idx = data.get("app_index", -1)
                env = _env_from_data(data)
                _clear_state(u["id"])
                _do_execute_now(m.chat.id, u, idx, text, env=env)

            elif step == "custom_input":
                # ✍️ المخصّص: "Event | Hours" → جدولة ببيانات المهمة
                parsed = _parse_custom(text)
                if not parsed:
                    bot.reply_to(m, _t("ask_delay", u["id"]),
                                 parse_mode="HTML", reply_markup=types.ForceReply(selective=False))
                    return  # نُبقي الحالة لإعادة المحاولة
                event, minutes = parsed
                idx = data.get("app_index", -1)
                env = _env_from_data(data)
                _clear_state(u["id"])
                status, result = _schedule_test(u, idx, event, minutes, env=env)
                if status == "ok":
                    when_txt = result.strftime("%Y-%m-%d %H:%M UTC") if hasattr(result, "strftime") else str(result)
                    bot.reply_to(m, _t("sched_ok", u["id"], event=html.escape(event), when=html.escape(when_txt)),
                                 parse_mode="HTML")
                elif status == "invalid":
                    bot.reply_to(m, _requirements_message(result, u["id"]))
                elif status == "balance":
                    bot.reply_to(m, _t("no_credits", u["id"]))
                else:
                    bot.reply_to(m, _t("sched_fail", u["id"]))
            elif step == "edit_value":
                job = _job_owned(data.get("job_id", 0), u)
                field = data.get("field")
                _clear_state(u["id"])
                if not job:
                    bot.reply_to(m, _t("session_over", u["id"]))
                elif _apply_job_edit(job, field, text):
                    bot.reply_to(m, _t("edit_done", u["id"]))
                else:
                    bot.reply_to(m, _t("bad_value", u["id"]))

            elif step == "support_msg":
                _clear_state(u["id"])
                _create_ticket(u["id"], text)        # silent inbox; admin reads via /admin
                bot.reply_to(m, _t("support_sent", u["id"]))

            # ── Admin-only text steps (guarded by role) ─────────────────────
            elif step == "admin_user_credit" and u["role"] == "admin":
                _clear_state(u["id"])
                try:
                    n = int(text)
                except ValueError:
                    bot.reply_to(m, _t("admin_bad_number", u["id"]))
                else:
                    _admin_add_credits(data.get("uid"), n)
                    bot.reply_to(m, _t("admin_credits_done", u["id"]))
                    _admin_show_user(m.chat.id, u, data.get("uid"))

            elif step == "admin_plan_edit" and u["role"] == "admin":
                _clear_state(u["id"])
                key, field = data.get("key"), data.get("field")
                if field in ("credits", "days"):
                    if not text.lstrip("-").isdigit():
                        bot.reply_to(m, _t("admin_bad_number", u["id"]))
                        _admin_show_plan(m.chat.id, u, key)
                        return
                    _update_plan(key, **{field: int(text)})
                else:                                  # price / label_en / label_ar / label_bn
                    _update_plan(key, **{field: text})
                bot.reply_to(m, _t("admin_plan_saved", u["id"]))
                _admin_show_plan(m.chat.id, u, key)

            elif step == "admin_plan_new" and u["role"] == "admin":
                _clear_state(u["id"])      # legacy path retired by the guided wizard below

            # ── Guided Add-Plan wizard: key → en → ar → bn → credits → days → price ──
            elif step == "pw_key" and u["role"] == "admin":
                key = re.sub(r"[^A-Za-z0-9]", "", text)[:16]
                if not key:
                    bot.reply_to(m, _t("pw_ask_key", u["id"]), parse_mode="HTML",
                                 reply_markup=types.ForceReply(selective=False))
                    return
                data["key"] = key
                _set_state(u["id"], "pw_en", data)
                bot.reply_to(m, _t("pw_ask_en", u["id"]), parse_mode="HTML",
                             reply_markup=types.ForceReply(selective=False))
            elif step == "pw_en" and u["role"] == "admin":
                data["en"] = text
                _set_state(u["id"], "pw_ar", data)
                bot.reply_to(m, _t("pw_ask_ar", u["id"]), parse_mode="HTML",
                             reply_markup=types.ForceReply(selective=False))
            elif step == "pw_ar" and u["role"] == "admin":
                data["ar"] = text
                _set_state(u["id"], "pw_bn", data)
                bot.reply_to(m, _t("pw_ask_bn", u["id"]), parse_mode="HTML",
                             reply_markup=types.ForceReply(selective=False))
            elif step == "pw_bn" and u["role"] == "admin":
                data["bn"] = text
                _set_state(u["id"], "pw_credits", data)
                bot.reply_to(m, _t("pw_ask_credits", u["id"]), parse_mode="HTML",
                             reply_markup=types.ForceReply(selective=False))
            elif step == "pw_credits" and u["role"] == "admin":
                if not text.isdigit():
                    bot.reply_to(m, _t("pw_ask_credits", u["id"]), parse_mode="HTML",
                                 reply_markup=types.ForceReply(selective=False))
                    return
                data["credits"] = int(text)
                _set_state(u["id"], "pw_days", data)
                bot.reply_to(m, _t("pw_ask_days", u["id"]), parse_mode="HTML",
                             reply_markup=types.ForceReply(selective=False))
            elif step == "pw_days" and u["role"] == "admin":
                if not text.isdigit():
                    bot.reply_to(m, _t("pw_ask_days", u["id"]), parse_mode="HTML",
                                 reply_markup=types.ForceReply(selective=False))
                    return
                data["days"] = int(text)
                _set_state(u["id"], "pw_price", data)
                bot.reply_to(m, _t("pw_ask_price", u["id"]), parse_mode="HTML",
                             reply_markup=types.ForceReply(selective=False))
            elif step == "pw_price" and u["role"] == "admin":
                _clear_state(u["id"])
                _create_plan_full(data["key"], data.get("en", data["key"]), data.get("ar", data["key"]),
                                  data.get("bn", data["key"]), data.get("credits", 0), data.get("days", 0), text)
                bot.reply_to(m, _t("plan_created2", u["id"], key=html.escape(data["key"])), parse_mode="HTML")
                _admin_show_plan(m.chat.id, u, data["key"])

            elif step == "admin_wallet" and u["role"] == "admin":
                _clear_state(u["id"])
                _set_setting("wallet_address", text)
                bot.reply_to(m, _t("admin_wallet_saved", u["id"]))

            elif step == "admin_broadcast" and u["role"] == "admin":
                _clear_state(u["id"])
                chat_ids = _all_user_chat_ids()
                announcement = text

                def _do_broadcast():
                    sent = 0
                    for cid in chat_ids:
                        try:
                            bot.send_message(int(cid), announcement)
                            sent += 1
                        except Exception:
                            pass
                    try:
                        bot.send_message(m.chat.id, _t("admin_broadcast_done", u["id"], n=sent))
                    except Exception:
                        pass

                _bg(_do_broadcast)         # async — never blocks the webhook thread

            else:
                # foolproof: unknown state → reset and return to the main menu
                _clear_state(u["id"])
                bot.send_message(m.chat.id, _t("main_menu", u["id"]), reply_markup=_main_menu_kb(u["id"]))
        except Exception as e:
            # always clear state so the user can never get stuck
            logger.error(f"[Telegram] state '{step}' error: {e}")
            try:
                _clear_state(u["id"])
            except Exception:
                pass
            bot.reply_to(m, _t("unexpected", u["id"]))

    # ── Catch-all: any text not matched above → reset & show the main menu ────
    # Registered last so commands, menu buttons and state-steps win first. This is
    # the final guarantee that a user can never get stuck on unexpected input.
    @bot.message_handler(func=lambda m: bool(m.text), content_types=["text"])
    def h_fallback(m):
        u = _user_by_chat(m.chat.id)
        if not u:
            bot.reply_to(m, locales.lookup("need_start"))
            return
        _clear_state(u["id"])
        bot.send_message(m.chat.id, _t("main_menu", u["id"]), reply_markup=_main_menu_kb(u["id"]))


_register_handlers()

# تجهيز جداول الاشتراكات/المدفوعات عند الإقلاع (idempotent)
if bot:
    _ensure_tables()


# ═══════════════════════════════════════════════════════════════════════════════
# تكامل Flask (Webhook)
# ═══════════════════════════════════════════════════════════════════════════════
_UPDATE_POOL = ThreadPoolExecutor(max_workers=12, thread_name_prefix="tg-update")


def _process_update_safe(update) -> None:
    """Runs the full handler chain off the webhook thread; logs but never raises."""
    try:
        bot.process_new_updates([update])
    except Exception as e:
        logger.error(f"[Telegram] update processing failed: {e}")


def register_webhook(app) -> None:
    if not bot:
        logger.info("[Telegram] webhook route not added (bot disabled).")
        return
    secret = config.TELEGRAM_WEBHOOK_SECRET or "hook"
    path = f"/telegram/webhook/{secret}"

    def _webhook():
        if config.TELEGRAM_WEBHOOK_SECRET:
            hdr = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if hdr != config.TELEGRAM_WEBHOOK_SECRET:
                return ("forbidden", 403)
        if request.headers.get("content-type", "").startswith("application/json"):
            update = types.Update.de_json(request.get_data().decode("utf-8"))
            # LATENCY FIX: do NOT process inline. Hand the update to a worker
            # thread and return 200 immediately (~1ms) so Telegram's spinner
            # clears at once; all DB/API work happens off the request thread.
            _UPDATE_POOL.submit(_process_update_safe, update)
            return ("", 200)
        return ("bad request", 400)

    app.add_url_rule(path, "telegram_webhook", _webhook, methods=["POST"])
    logger.info(f"[Telegram] webhook route registered: {path}")


_BOT_COMMANDS = [
    ("start",    "cmd_start"),
    ("apps",     "cmd_apps"),
    ("jobs",     "cmd_jobs"),
    ("balance",  "cmd_balance"),
    ("services", "cmd_services"),
    ("profile",  "cmd_profile"),
    ("help",     "cmd_help"),
]


def _setup_commands() -> None:
    """Registers the persistent command menu (the 'Menu' button) per language."""
    if not bot:
        return
    try:
        default_cmds = [types.BotCommand(c, locales.lookup(key, locales.DEFAULT_LANG))
                        for c, key in _BOT_COMMANDS]
        bot.set_my_commands(default_cmds)                       # default scope (all users)
        for lang in locales.SUPPORTED:
            if lang == locales.DEFAULT_LANG:
                continue
            cmds = [types.BotCommand(c, locales.lookup(key, lang)) for c, key in _BOT_COMMANDS]
            bot.set_my_commands(cmds, language_code=lang)       # per-language descriptions
        logger.info("[Telegram] bot command menu configured")
    except Exception as e:
        logger.warning(f"[Telegram] set_my_commands failed: {e}")


def maybe_setup_webhook() -> None:
    if not bot:
        return
    _setup_commands()                       # persistent Menu button — set regardless of webhook
    base = config.PUBLIC_BASE_URL
    if not base:
        logger.info("[Telegram] PUBLIC_BASE_URL not set — use scripts/set_webhook.py")
        return
    secret = config.TELEGRAM_WEBHOOK_SECRET or "hook"
    url = f"{base.rstrip('/')}/telegram/webhook/{secret}"
    try:
        bot.remove_webhook()
        bot.set_webhook(url=url, secret_token=(config.TELEGRAM_WEBHOOK_SECRET or None), drop_pending_updates=False)
        logger.info(f"[Telegram] webhook set → {url}")
    except Exception as e:
        logger.warning(f"[Telegram] setWebhook failed: {e}")
