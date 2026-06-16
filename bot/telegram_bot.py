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
TEXT_STEPS = {"device_gaid", "device_idfa", "device_afid", "proxy", "sniper_value", "custom_input",
              "task_gaid", "task_idfa", "task_afid",
              "add_name", "add_package", "add_devkey", "add_events",
              "edit_value", "support_msg"}


def _default_dev_key() -> str:
    return getattr(config, "DEFAULT_DEV_KEY", "") or os.environ.get("DEFAULT_DEV_KEY", "")


# ═══════════════════════════════════════════════════════════════════════════════
# مستخدمون
# ═══════════════════════════════════════════════════════════════════════════════
def _user_by_chat(chat_id) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE tg_chat_id = %s AND active = 1", (str(chat_id),))
            return cur.fetchone()


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


def _clear_chat(chat_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET tg_chat_id=NULL WHERE tg_chat_id=%s", (str(chat_id),))


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
PLANS = {
    "w": {"days": 7,  "credits": 50,  "label_ar": "أسبوعي", "label_en": "Weekly"},
    "m": {"days": 30, "credits": 200, "label_ar": "شهري",   "label_en": "Monthly"},
}

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
                cur.execute("CREATE INDEX IF NOT EXISTS idx_sub_user ON subscriptions(user_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_pay_status ON payments(status)")
        _tables_ready = True
    except Exception as e:
        logger.warning(f"[Telegram] ensure tables failed: {e}")


def _plan_label(plan_key, uid=None):
    p = PLANS.get(plan_key)
    if not p:
        return plan_key
    return p["label_en"] if (uid and _lang(uid) == "en") else p["label_ar"]


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
    """يفعّل اشتراكاً ويضيف الرصيد (يُستدعى عند قبول الدفع)."""
    p = PLANS.get(plan_key)
    if not p:
        return None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO subscriptions (user_id, plan, status, expires_at)
                   VALUES (%s,%s,'active', NOW() + make_interval(days => %s))
                   RETURNING expires_at""",
                (user_id, plan_key, p["days"]),
            )
            expires = cur.fetchone()["expires_at"]
            cur.execute(
                "UPDATE users SET uses_left = uses_left + %s, max_uses = max_uses + %s WHERE id=%s",
                (p["credits"], p["credits"], user_id),
            )
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
    return (True, row["uses_left"]) if row else (False, 0)


def _refund_use(user):
    if user["role"] == "admin":
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET uses_left = uses_left + 1 WHERE id=%s", (user["id"],))


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
    name = f"{app_cfg['name']} · {event_name}"
    events = json.dumps([{"name": event_name}])

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO scheduled_jobs
                         (user_id, name, events, package, dev_key, gaid, afid, os,
                          proxy_host, proxy_port, proxy_user, proxy_pass,
                          run_at, enabled)
                       VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                               NOW() + make_interval(mins => %s), 1)
                       RETURNING run_at""",
                    (user["id"], name, events, app_cfg["package"], dev_key,
                     device_id, env.get("afid", ""), os_,
                     p_host, p_port, p_user, p_pass, minutes),
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
    events = json.dumps([{"name": e} for e in data["events"]])
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO scheduled_jobs
                         (user_id, name, events, package, dev_key, gaid, afid, os,
                          proxy_host, proxy_port, proxy_user, proxy_pass, run_at, enabled)
                       VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW(), 1)
                       RETURNING id""",
                    (user["id"], data["name"], events, data["package"], data["dev_key"],
                     device_id, env.get("afid", ""), os_, p_host, p_port, p_user, p_pass),
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
    bot.send_message(chat_id, _t("services_menu", u["id"]), reply_markup=kb)


def _send_plans(chat_id, u):
    kb = types.InlineKeyboardMarkup()
    for key, p in PLANS.items():
        label = f"{_plan_label(key, u['id'])} · {p['credits']}/{p['days']}d"
        kb.row(types.InlineKeyboardButton(label, callback_data=f"pay:plan:{key}"))
    kb.row(types.InlineKeyboardButton(_t("btn_cancel", u["id"]), callback_data="svc:close"))
    bot.send_message(chat_id, _t("choose_plan", u["id"]), reply_markup=kb)


def _send_sub_status(chat_id, u):
    expiry = _sub_expiry(u["id"])
    fresh = _user_by_chat(chat_id) or u
    if expiry:
        until = expiry.strftime("%Y-%m-%d") if hasattr(expiry, "strftime") else str(expiry)
        bot.send_message(chat_id, _t("sub_active", u["id"], until=until, credits=fresh["uses_left"]), parse_mode="HTML")
    else:
        bot.send_message(chat_id, _t("sub_none", u["id"], credits=fresh["uses_left"]))


def _notify_admins_payment(payment_id, user, plan_key, screenshot):
    """Sends the payment screenshot to each admin with approve/reject buttons."""
    caption = _t("admin_pay_request", 0, id=payment_id,
                 user=f"{user['username']} (#{user['id']})", plan=_plan_label(plan_key))
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(_t("btn_approve", 0), callback_data=f"pay:approve:{payment_id}"),
        types.InlineKeyboardButton(_t("btn_reject", 0),  callback_data=f"pay:reject:{payment_id}"),
    )
    for cid in _admin_chat_ids():
        try:
            bot.send_photo(cid, screenshot, caption=caption, reply_markup=kb)
        except Exception as e:
            logger.warning(f"[Telegram] notify admin {cid} failed: {e}")


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
    if not jobs:
        bot.send_message(chat_id, _t("jobs_empty", u["id"]))
        return
    kb = types.InlineKeyboardMarkup()
    for j in jobs:
        mark = "✅" if j["enabled"] else "⏸️"
        kb.row(types.InlineKeyboardButton(f"{mark} {j['name']}", callback_data=f"job:view:{j['id']}"))
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
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id)
            return
        parts = c.data.split(":")
        action = parts[1] if len(parts) > 1 else ""
        # اختيار باقة → تعليمات + انتظار لقطة الشاشة
        if action == "plan" and len(parts) > 2:
            plan_key = parts[2]
            p = PLANS.get(plan_key)
            if not p:
                bot.answer_callback_query(c.id)
                return
            _set_state(u["id"], "pay_screenshot", {"plan": plan_key})
            bot.answer_callback_query(c.id)
            bot.send_message(
                c.message.chat.id,
                _t("pay_instructions", u["id"], plan=_plan_label(plan_key, u["id"]),
                   credits=p["credits"], days=p["days"]),
                parse_mode="HTML",
            )
            return
        # قبول/رفض (للمشرفين فقط)
        if action in ("approve", "reject") and len(parts) > 2 and u["role"] == "admin":
            pid = int(parts[2]) if parts[2].isdigit() else 0
            row = _set_payment_status(pid, "approved" if action == "approve" else "rejected")
            bot.answer_callback_query(c.id, _t("pay_done", u["id"]))
            if not row:
                bot.send_message(c.message.chat.id, _t("pay_not_found", u["id"]))
                return
            target = _user_by_id(row["user_id"])
            if action == "approve":
                expires = _grant_subscription(row["user_id"], row["plan"])
                until = expires.strftime("%Y-%m-%d") if hasattr(expires, "strftime") else str(expires)
                p = PLANS.get(row["plan"], {})
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
        bot.answer_callback_query(c.id)

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
            _notify_admins_payment(pid, u, plan_key, file_id)
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

    # ── /jobs — قائمة المهام + التحرير (المطلب 8) ────────────────────────────
    @bot.message_handler(commands=["jobs"])
    @linked
    def h_jobs(m, u):
        _open_jobs(m.chat.id, u)

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
            _set_state(u["id"], "proxy", {})
            bot.send_message(
                c.message.chat.id, _t("proxy_prompt", u["id"]),
                reply_markup=types.ForceReply(selective=False),
            )

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
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id, _t("need_start", 0))
            return
        _set_notify(u["id"], not _get_notify(u["id"]))
        bot.answer_callback_query(c.id, _t("updated", u["id"]))
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
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id, _t("need_start", 0))
            return
        bot.answer_callback_query(c.id)
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
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id, _t("need_start", 0))
            return
        action = c.data.split(":", 1)[1]
        if action == "cancel":
            _clear_state(u["id"])
            bot.answer_callback_query(c.id)
            bot.send_message(c.message.chat.id, _t("reset_done", u["id"]), reply_markup=_main_menu_kb(u["id"]))
            return
        step, data = _get_state(u["id"])
        if step != "awaiting_mode":
            bot.answer_callback_query(c.id, _t("session_over", u["id"]))
            return
        if action == "sniper":
            _set_state(u["id"], "sniper_value", data)
            bot.answer_callback_query(c.id)
            bot.send_message(c.message.chat.id, _t("ask_event", u["id"]),
                             parse_mode="HTML", reply_markup=types.ForceReply(selective=False))
        elif action == "custom":
            _set_state(u["id"], "custom_input", data)
            bot.answer_callback_query(c.id)
            bot.send_message(c.message.chat.id, _t("ask_delay", u["id"]),
                             parse_mode="HTML", reply_markup=types.ForceReply(selective=False))

    # ── أزرار المهام القديمة ──────────────────────────────────────────────────
    @bot.callback_query_handler(func=lambda c: (c.data or "").split(":")[0] in ("run", "tog", "del"))
    def h_job_cb(c):
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id, _t("need_start", 0))
            return
        action, _, sid = (c.data or "").partition(":")
        if not sid.isdigit():
            bot.answer_callback_query(c.id, _t("unknown_cmd", u["id"]))
            return
        jid = int(sid)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM scheduled_jobs WHERE id=%s", (jid,))
                job = cur.fetchone()
        if not job or (job["user_id"] != u["id"] and u["role"] != "admin"):
            bot.answer_callback_query(c.id, _t("not_allowed", u["id"]))
            return
        if action == "run":
            execute_job.apply_async(args=[jid], countdown=0)
            bot.answer_callback_query(c.id, _t("job_run_toast", u["id"]))
        elif action == "tog":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE scheduled_jobs SET enabled = 1 - enabled WHERE id=%s", (jid,))
            bot.answer_callback_query(c.id, _t("job_toggled", u["id"]))
        elif action == "del":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM scheduled_jobs WHERE id=%s", (jid,))
            bot.answer_callback_query(c.id, _t("job_deleted", u["id"]))

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

            elif step == "proxy":
                if text:
                    _save_env(u["id"], proxy=text)
                    msg = _t("proxy_saved", u["id"])
                else:
                    env_now = _get_env(u["id"])
                    env_now.pop("proxy", None)
                    _ud_set(u["id"], "tg_env", json.dumps(env_now))
                    msg = _t("proxy_cleared", u["id"])
                _clear_state(u["id"])
                bot.reply_to(m, msg)

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
                for cid in _admin_chat_ids():
                    try:
                        bot.send_message(int(cid), _t("support_from", 0, user=u["username"], id=u["id"], text=text))
                    except Exception:
                        pass
                bot.reply_to(m, _t("support_sent", u["id"]))

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
            bot.process_new_updates([update])
            return ("", 200)
        return ("bad request", 400)

    app.add_url_rule(path, "telegram_webhook", _webhook, methods=["POST"])
    logger.info(f"[Telegram] webhook route registered: {path}")


def maybe_setup_webhook() -> None:
    if not bot:
        return
    base = config.PUBLIC_BASE_URL
    if not base:
        logger.info("[Telegram] PUBLIC_BASE_URL غير مضبوط — استخدم scripts/set_webhook.py")
        return
    secret = config.TELEGRAM_WEBHOOK_SECRET or "hook"
    url = f"{base.rstrip('/')}/telegram/webhook/{secret}"
    try:
        bot.remove_webhook()
        bot.set_webhook(url=url, secret_token=(config.TELEGRAM_WEBHOOK_SECRET or None), drop_pending_updates=False)
        logger.info(f"[Telegram] webhook set → {url}")
    except Exception as e:
        logger.warning(f"[Telegram] setWebhook failed: {e}")
