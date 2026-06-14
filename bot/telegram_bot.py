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
TEXT_STEPS = {"device_gaid", "device_idfa", "device_afid", "proxy", "app_value", "schedule_delay",
              "add_name", "add_package", "add_devkey", "add_events"}


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
        missing.append("dev_key")            # إعداد مشرف (ليس من المستخدم)
    os_ = (env.get("os") or "").strip().lower()
    if os_ not in ("android", "ios"):
        missing.append("نظام التشغيل")
    elif os_ == "ios" and not (env.get("idfa") or "").strip():
        missing.append("IDFA")
    elif os_ == "android" and not (env.get("gaid") or "").strip():
        missing.append("GAID")
    if not (env.get("afid") or "").strip():
        missing.append("AFID")
    return missing


def _requirements_message(missing):
    """رسالة مناسبة: نقص إعداد المشرف (dev_key) أم نقص إعداد جهاز المستخدم."""
    if "dev_key" in missing:
        return "⚠️ هذا التطبيق غير مُهيّأ للإرسال بعد (مفتاح التطوير مفقود). تواصل مع المشرف."
    return "⚙️ أكمل إعداد جهازك عبر /profile.\nينقص: " + "، ".join(missing)


def _dispatch_event(app_cfg, value, user, env):
    """يرسل حدث AppsFlyer ببيانات المختبِر. يعيد (ok, info, transport_error)."""
    package = app_cfg["package"]
    dev_key = app_cfg.get("dev_key") or _default_dev_key()
    if not (dev_key or "").strip():
        # دفاع: لا نرسل طلباً بلا مفتاح. transport_error=True ⇒ يُعاد الرصيد.
        logger.warning(f"[Telegram] dispatch aborted: missing dev_key for {package}")
        return False, "dev_key مفقود", True
    event_name = app_cfg.get("event", "af_level_achieved")
    os_ = (env.get("os") or "").lower()

    body = {
        "appsflyer_id": env.get("afid", "") or app_cfg.get("afid", ""),
        "eventName": event_name,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "eventValue": json.dumps({"value": value}),
    }
    if os_ == "ios":
        body["idfa"] = env.get("idfa", "")
    else:
        body["advertising_id"] = env.get("gaid", "") or app_cfg.get("gaid", "")

    proxies = _proxies_from_string(env.get("proxy", "")) or _build_proxies(
        app_cfg.get("proxy_host", ""), app_cfg.get("proxy_port", ""),
        app_cfg.get("proxy_user", ""), app_cfg.get("proxy_pass", ""),
    )
    try:
        r = requests.post(
            f"https://api2.appsflyer.com/inappevent/{package}",
            headers={"Content-Type": "application/json", "authentication": dev_key or ""},
            json=body, proxies=proxies, timeout=15,
        )
        ok = r.status_code in (200, 201)
        _log_event_history(user["id"], package, event_name, r.status_code, ok)
        return ok, f"HTTP {r.status_code}", False
    except requests.RequestException as e:
        _log_event_history(user["id"], package, event_name, 0, False)
        return False, str(e)[:80], True


def _do_execute_now(chat_id, user, idx, value):
    if idx < 0 or idx >= len(GAMES_DATA):
        bot.send_message(chat_id, "انتهت الجلسة. أعد /apps.")
        return
    app_cfg = GAMES_DATA[idx]
    env = _get_env(user["id"])
    # بوابة الصدّ: تحقّق قبل خصم الرصيد
    missing = _missing_requirements(app_cfg, env)
    if missing:
        bot.send_message(chat_id, _requirements_message(missing))
        return
    ok_bal, left = _consume_use(user)
    if not ok_bal:
        bot.send_message(chat_id, "🚫 عذراً، نفد رصيدك.")
        return
    bot.send_chat_action(chat_id, "typing")
    ok, info, transport_err = _dispatch_event(app_cfg, value, user, env)
    if not ok and transport_err:
        _refund_use(user)
        left += 1
    if ok:
        bot.send_message(chat_id, f"✅ تم تنفيذ الاختبار.\n{app_cfg['name']} · القيمة: {value}\nالرصيد: {left}")
    else:
        bot.send_message(chat_id, f"❌ فشل التنفيذ ({info}).\nالرصيد: {left}")


_DELAY_RE = re.compile(r"^\s*(\d+)\s*([hHdD])\s*$")


def _parse_delay_minutes(text):
    m = _DELAY_RE.match(text or "")
    if not m:
        return None
    n, unit = int(m.group(1)), m.group(2).lower()
    if n <= 0:
        return None
    return n * 60 if unit == "h" else n * 1440


def _schedule_test(user, idx, value, minutes):
    """
    يجدول اختباراً. يعيد (status, payload):
      ("ok", run_at) | ("invalid", missing_list) | ("balance", None)
      | ("session", None) | ("db", None)
    يتحقّق من الجاهزية قبل خصم الرصيد أو الإدراج (لا تلويث لقاعدة البيانات).
    """
    if idx < 0 or idx >= len(GAMES_DATA):
        return "session", None
    app_cfg = GAMES_DATA[idx]
    env = _get_env(user["id"])

    missing = _missing_requirements(app_cfg, env)
    if missing:
        return "invalid", missing

    ok_bal, _ = _consume_use(user)
    if not ok_bal:
        return "balance", None

    event_name = app_cfg.get("event", "af_level_achieved")
    dev_key = app_cfg.get("dev_key") or _default_dev_key()
    os_ = (env.get("os") or "").lower()
    device_id = env.get("idfa", "") if os_ == "ios" else env.get("gaid", "")
    p_host, p_port, p_user, p_pass = _proxy_parts(env.get("proxy", ""))
    name = f"{app_cfg['name']} · {event_name}={value}"
    events = json.dumps([{"name": event_name}])

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO scheduled_jobs
                         (user_id, name, events, package, dev_key, gaid, afid,
                          proxy_host, proxy_port, proxy_user, proxy_pass,
                          run_at, enabled)
                       VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,
                               NOW() + make_interval(mins => %s), 1)
                       RETURNING run_at""",
                    (user["id"], name, events, app_cfg["package"], dev_key,
                     device_id, env.get("afid", ""),
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
            bot.reply_to(message, "أرسل /start أولاً لإنشاء حسابك.")
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


def _send_add_step(chat_id, step, data, err=None):
    """يرسل رسالة الخطوة (HTML) مع الكيبورد. err اختياري للتحقّق الفاشل."""
    kb = types.InlineKeyboardMarkup()
    prefix = f"⚠️ {html.escape(err)}\n\n" if err else ""
    if step == "add_name":
        text = (prefix + "➕ <b>إنشاء مهمة جديدة</b>\n\n"
                f"<code>{_step_bar(1)}</code>  <b>الخطوة 1 من 5</b>\n"
                "أرسل <b>اسم المهمة</b> (إنجليزي بدون مسافات):\n\n"
                "مثال: <code>game_launch_v2</code>")
        kb.row(types.InlineKeyboardButton("❌ إلغاء", callback_data="add:cancel"))
    elif step == "add_package":
        text = (prefix + "✅ <b>تم حفظ الاسم</b>\n\n"
                f"<code>{_step_bar(2)}</code>  <b>الخطوة 2 من 5</b>\n"
                "أرسل <b>اسم الحزمة</b> (Package Name):\n\n"
                "مثال: <code>com.example.app</code>")
        for ex in _ADD_EX["add_package"]:
            kb.row(types.InlineKeyboardButton(ex, callback_data=f"add:exv:{ex}"))
        kb.row(types.InlineKeyboardButton("🔙 السابق", callback_data="add:back"),
               types.InlineKeyboardButton("❌ إلغاء", callback_data="add:cancel"))
    elif step == "add_devkey":
        text = (prefix + "✅ تم حفظ الحزمة\n\n"
                f"<code>{_step_bar(3)}</code>  <b>الخطوة 3 من 5</b>\n"
                "أرسل <b>Dev Key</b> الخاص بتطبيقك من لوحة AppsFlyer:")
        kb.row(types.InlineKeyboardButton("🔙 السابق", callback_data="add:back"),
               types.InlineKeyboardButton("❌ إلغاء", callback_data="add:cancel"))
    elif step == "add_events":
        text = (prefix + "✅ تم حفظ المفتاح\n\n"
                f"<code>{_step_bar(4)}</code>  <b>الخطوة 4 من 5</b>\n"
                "أرسل <b>أسماء الأحداث</b> مفصولة بفاصلة:\n\n"
                "مثال: <code>af_launch,af_login,af_purchase</code>")
        for ex in _ADD_EX["add_events"]:
            kb.row(types.InlineKeyboardButton(ex, callback_data=f"add:exv:{ex}"))
        kb.row(types.InlineKeyboardButton("🔙 السابق", callback_data="add:back"),
               types.InlineKeyboardButton("❌ إلغاء", callback_data="add:cancel"))
    else:
        return
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)


def _send_review(chat_id, user, data):
    """الخطوة 5: مراجعة المهمة مع معاينة Payload (يشبه صندوق التصميم)."""
    env = _get_env(user["id"])
    os_ = (env.get("os") or "").lower()
    afid = env.get("afid", "") or "—"
    dev_id = (env.get("idfa", "") if os_ == "ios" else env.get("gaid", "")) or "—"
    id_field = "idfa" if os_ == "ios" else "advertising_id"
    first_ev = data["events"][0] if data.get("events") else "af_event"

    name     = html.escape(data.get("name", ""))
    package  = html.escape(data.get("package", ""))
    dev_mask = html.escape(data.get("dev_key", "")[:6] + "…") if data.get("dev_key") else "—"
    events_s = html.escape("، ".join(data.get("events", [])))

    payload_raw = (
        "{\n"
        f'  "appsflyer_id": "{afid}",\n'
        f'  "eventName": "{first_ev}",\n'
        '  "eventTime": "<auto>",\n'
        '  "eventValue": "{}",\n'
        f'  "{id_field}": "{dev_id}"\n'
        "}"
    )
    text = (
        f"<code>{_step_bar(5)}</code>  <b>الخطوة 5 من 5</b>\n"
        "✅ <b>مراجعة المهمة قبل الحفظ</b>\n\n"
        f"📌 الاسم: <b>{name}</b>\n"
        f"📦 الحزمة: <b>{package}</b>\n"
        f"🔑 Dev Key: <b>{dev_mask}</b>\n"
        f"📡 الأحداث: <b>{events_s}</b>\n\n"
        "<b>Payload (معاينة):</b>\n"
        f"<code>{html.escape(payload_raw)}</code>\n\n"
        "هل تريد حفظ المهمة؟"
    )
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("✅ حفظ المهمة", callback_data="add:save"),
           types.InlineKeyboardButton("🔄 إعادة البدء", callback_data="add:restart"))
    kb.row(types.InlineKeyboardButton("❌ إلغاء", callback_data="add:cancel"))
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)


def _add_advance(chat_id, user, value):
    """يعالج إدخال الخطوة الحالية في /add (من نص أو زر مثال) مع التحقّق."""
    step, data = _get_state(user["id"])
    value = (value or "").strip()
    if step == "add_name":
        if not re.fullmatch(r"[A-Za-z0-9_]{2,40}", value):
            _send_add_step(chat_id, "add_name", data, err="الاسم إنجليزي بدون مسافات (2–40: حروف/أرقام/_).")
            return
        data["name"] = value
        _set_state(user["id"], "add_package", data)
        _send_add_step(chat_id, "add_package", data)
    elif step == "add_package":
        if "." not in value or not re.fullmatch(r"[A-Za-z0-9_.]{3,80}", value):
            _send_add_step(chat_id, "add_package", data, err="حزمة غير صالحة. مثال: com.example.app")
            return
        data["package"] = value
        _set_state(user["id"], "add_devkey", data)
        _send_add_step(chat_id, "add_devkey", data)
    elif step == "add_devkey":
        if len(value) < 6:
            _send_add_step(chat_id, "add_devkey", data, err="Dev Key قصير جداً.")
            return
        data["dev_key"] = value
        _set_state(user["id"], "add_events", data)
        _send_add_step(chat_id, "add_events", data)
    elif step == "add_events":
        evs = [e.strip() for e in value.split(",") if e.strip()]
        if not evs:
            _send_add_step(chat_id, "add_events", data, err="أرسل اسم حدث واحداً على الأقل.")
            return
        data["events"] = evs
        _set_state(user["id"], "awaiting_confirm", data)
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
                         (user_id, name, events, package, dev_key, gaid, afid,
                          proxy_host, proxy_port, proxy_user, proxy_pass, run_at, enabled)
                       VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s, NOW(), 1)
                       RETURNING id""",
                    (user["id"], data["name"], events, data["package"], data["dev_key"],
                     device_id, env.get("afid", ""), p_host, p_port, p_user, p_pass),
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
        f"إشعارات النتائج: {'مفعّلة ✅' if on else 'موقوفة ⛔'}",
        callback_data="settings:toggle",
    ))
    return kb


def _open_apps(chat_id, user):
    if not GAMES_DATA:
        bot.send_message(chat_id, "لا توجد تطبيقات مُعرّفة بعد.")
        return
    kb = types.InlineKeyboardMarkup()
    row = []
    for i, app_cfg in enumerate(GAMES_DATA):
        row.append(types.InlineKeyboardButton(app_cfg["name"], callback_data=f"app:{i}"))
        if len(row) == 2:
            kb.row(*row); row = []
    if row:
        kb.row(*row)
    bot.send_message(chat_id, "اختر التطبيق للاختبار:", reply_markup=kb)


def _open_profile(chat_id, user):
    env = _get_env(user["id"])
    os_ = env.get("os", "—")
    dev_id = env.get("idfa", "") if os_ == "ios" else env.get("gaid", "")
    dev_lbl = "IDFA" if os_ == "ios" else "GAID"
    txt = (
        f"🧪 بيئة اختبار {user['username']}\n\n"
        f"النظام: {os_ or '—'}\n"
        f"{dev_lbl}: {dev_id or '—'}\n"
        f"AFID: {env.get('afid','') or '—'}\n"
        f"البروكسي: {env.get('proxy','') or '—'}"
    )
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("📱 تحديث الجهاز", callback_data="profile:device"),
        types.InlineKeyboardButton("🌐 تحديث البروكسي", callback_data="profile:proxy"),
    )
    bot.send_message(chat_id, txt, reply_markup=kb)


MAIN_MENU_LABELS = {
    "➕ مهمة جديدة": "add",
    "🧪 التطبيقات":  "apps",
    "👤 حسابي":      "profile",
    "⚙️ الإعدادات":  "settings",
    "📊 رصيدي":      "balance",
}


def _main_menu_kb():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("➕ مهمة جديدة", "🧪 التطبيقات")
    kb.row("👤 حسابي", "⚙️ الإعدادات")
    kb.row("📊 رصيدي")
    return kb


def _register_handlers():
    if not bot:
        return

    # ── /start ───────────────────────────────────────────────────────────────
    @bot.message_handler(commands=["start"])
    def h_start(m):
        parts = (m.text or "").split(maxsplit=1)
        u = _user_by_chat(m.chat.id)
        if not u and len(parts) == 2:
            uid = _consume_link_code(parts[1].strip().upper())
            if uid:
                _set_chat(uid, m.chat.id)
                u = _user_by_chat(m.chat.id)
                bot.reply_to(m, f"✅ تم ربط حسابك {u['username']}.\n/apps للبدء.")
                return
        if u:
            bot.send_message(
                m.chat.id,
                f"مرحباً {u['username']} 👋\nالرصيد: {u['uses_left']}/{u['max_uses']}",
                reply_markup=_main_menu_kb(),
            )
            return
        u = _auto_register(m.chat.id, m.from_user.username, m.from_user.first_name)
        bot.send_message(
            m.chat.id,
            f"أهلاً {u['username']} 👋\nتم إنشاء حسابك.\n🎁 رصيدك المجاني: {u['uses_left']} اختبارات.\n\n"
            f"1) أعدّ جهازك: /profile\n2) ابدأ اختباراً: /apps أو ➕ مهمة جديدة",
            reply_markup=_main_menu_kb(),
        )

    @bot.message_handler(commands=["help"])
    def h_help(m):
        bot.reply_to(
            m,
            "الأوامر:\n/add — إنشاء مهمة مجدولة (خطوات)\n/apps — بدء اختبار سريع\n"
            "/profile — بيئة الاختبار (جهاز/بروكسي)\n/settings — الإشعارات\n"
            "/balance — الرصيد\n/history — السجلّ\n/status — حالتك\n/unlink — فكّ الربط",
        )

    @bot.message_handler(commands=["unlink"])
    def h_unlink(m):
        _clear_chat(m.chat.id)
        bot.reply_to(m, "تم فكّ الربط. أرسل /start للبدء مجدداً.")

    @bot.message_handler(commands=["balance"])
    @linked
    def h_balance(m, u):
        bot.reply_to(m, f"💳 رصيدك: {u['uses_left']} من {u['max_uses']}")

    @bot.message_handler(commands=["status"])
    @linked
    def h_status(m, u):
        bot.reply_to(m, f"👤 {u['username']}\nالدور: {u['role']}\nالرصيد: {u['uses_left']}/{u['max_uses']}")

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
            bot.reply_to(m, "لا يوجد سجلّ بعد.")
            return
        lines = []
        for r in rows:
            mark = "✅" if r["ok"] else "❌"
            ts = r["created_at"].strftime("%m-%d %H:%M") if r["created_at"] else ""
            lines.append(f"{mark} {r['event_name']} → {r['status']}  {ts}")
        bot.reply_to(m, "آخر العمليات:\n" + "\n".join(lines))

    # ── /profile ─────────────────────────────────────────────────────────────
    @bot.message_handler(commands=["profile"])
    @linked
    def h_profile(m, u):
        _open_profile(m.chat.id, u)

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("profile:"))
    def h_profile_cb(c):
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id, "أرسل /start أولاً")
            return
        action = c.data.split(":", 1)[1]
        if action == "device":
            bot.answer_callback_query(c.id)
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("🤖 Android", callback_data="os:android"),
                types.InlineKeyboardButton("🍎 iOS", callback_data="os:ios"),
            )
            bot.send_message(c.message.chat.id, "اختر نظام التشغيل لجهاز الاختبار:", reply_markup=kb)
        elif action == "proxy":
            _set_state(u["id"], "proxy", {})
            bot.answer_callback_query(c.id)
            bot.send_message(
                c.message.chat.id,
                "أرسل البروكسي (host:port أو user:pass@host:port):",
                reply_markup=types.ForceReply(selective=False),
            )

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("os:"))
    def h_os_cb(c):
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id, "أرسل /start أولاً")
            return
        os_ = c.data.split(":", 1)[1]
        bot.answer_callback_query(c.id)
        if os_ == "android":
            _set_state(u["id"], "device_gaid", {"os": "android"})
            bot.send_message(c.message.chat.id, "أرسل GAID:", reply_markup=types.ForceReply(selective=False))
        else:
            _set_state(u["id"], "device_idfa", {"os": "ios"})
            bot.send_message(c.message.chat.id, "أرسل IDFA:", reply_markup=types.ForceReply(selective=False))

    # ── /settings ────────────────────────────────────────────────────────────
    @bot.message_handler(commands=["settings"])
    @linked
    def h_settings(m, u):
        bot.send_message(m.chat.id, "⚙️ الإعدادات:", reply_markup=_settings_kb(u["id"]))

    @bot.callback_query_handler(func=lambda c: c.data == "settings:toggle")
    def h_settings_cb(c):
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id, "أرسل /start أولاً")
            return
        _set_notify(u["id"], not _get_notify(u["id"]))
        bot.answer_callback_query(c.id, "تم التحديث")
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
        _send_add_step(m.chat.id, "add_name", {})

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("add:"))
    def h_add_cb(c):
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id, "أرسل /start أولاً")
            return
        action = c.data.split(":", 1)[1]
        if action == "cancel":
            _clear_state(u["id"])
            bot.answer_callback_query(c.id, "أُلغي")
            bot.send_message(c.message.chat.id, "❌ تم إلغاء إنشاء المهمة.")
            return
        if action == "restart":
            _set_state(u["id"], "add_name", {})
            bot.answer_callback_query(c.id)
            _send_add_step(c.message.chat.id, "add_name", {})
            return
        if action == "back":
            step, data = _get_state(u["id"])
            prev = _ADD_PREV.get(step)
            bot.answer_callback_query(c.id)
            if prev:
                _set_state(u["id"], prev, data)
                _send_add_step(c.message.chat.id, prev, data)
            return
        if action == "save":
            step, data = _get_state(u["id"])
            if step != "awaiting_confirm":
                bot.answer_callback_query(c.id, "انتهت الجلسة")
                return
            if not all(data.get(k) for k in ("name", "package", "dev_key", "events")):
                _clear_state(u["id"])
                bot.answer_callback_query(c.id)
                bot.send_message(c.message.chat.id, "بيانات ناقصة. أعد /add.")
                return
            jid = _save_add_job(u, data)
            _clear_state(u["id"])
            bot.answer_callback_query(c.id, "تم الحفظ")
            if jid:
                env = _get_env(u["id"])
                ready = (env.get("os") and (env.get("gaid") or env.get("idfa")) and env.get("afid"))
                note = "" if ready else "\n\n⚠️ بيئة جهازك غير مكتملة — أكملها عبر /profile قبل التشغيل."
                bot.send_message(
                    c.message.chat.id,
                    f"✅ <b>تم حفظ المهمة</b> (#{jid}).\nستُنفَّذ خلال دقيقة عند أقرب مسح.{note}",
                    parse_mode="HTML",
                )
            else:
                bot.send_message(c.message.chat.id, "❌ تعذّر حفظ المهمة. حاول لاحقاً.")
            return
        if action.startswith("exv:"):
            value = action[len("exv:"):]
            bot.answer_callback_query(c.id)
            _add_advance(c.message.chat.id, u, value)
            return

    # ── القائمة الرئيسية (ReplyKeyboard) — تربط الأزرار بالأوامر ──────────────
    @bot.message_handler(
        func=lambda m: bool(m.text) and m.text in MAIN_MENU_LABELS and _text_step(m.chat.id) is None,
        content_types=["text"],
    )
    def h_menu(m):
        u = _user_by_chat(m.chat.id)
        if not u:
            bot.reply_to(m, "أرسل /start أولاً.")
            return
        target = MAIN_MENU_LABELS[m.text]
        if target == "add":
            _set_state(u["id"], "add_name", {})
            _send_add_step(m.chat.id, "add_name", {})
        elif target == "apps":
            _open_apps(m.chat.id, u)
        elif target == "profile":
            _open_profile(m.chat.id, u)
        elif target == "settings":
            bot.send_message(m.chat.id, "⚙️ الإعدادات:", reply_markup=_settings_kb(u["id"]))
        elif target == "balance":
            bot.send_message(m.chat.id, f"💳 رصيدك: {u['uses_left']} من {u['max_uses']}")

    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("app:"))
    def h_app_pick(c):
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id, "أرسل /start أولاً")
            return
        try:
            idx = int(c.data.split(":", 1)[1])
        except (ValueError, IndexError):
            bot.answer_callback_query(c.id, "اختيار غير صالح")
            return
        if idx < 0 or idx >= len(GAMES_DATA):
            bot.answer_callback_query(c.id, "تطبيق غير موجود")
            return
        _set_state(u["id"], "app_value", {"app_index": idx})
        bot.answer_callback_query(c.id)
        bot.send_message(
            c.message.chat.id,
            f"📲 {GAMES_DATA[idx]['name']}\nأرسل رقم المستوى/قيمة الحدث:",
            reply_markup=types.ForceReply(selective=False),
        )

    # ── متى التنفيذ؟ (أزرار) ──────────────────────────────────────────────────
    @bot.callback_query_handler(func=lambda c: (c.data or "").startswith("when:"))
    def h_when_cb(c):
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id, "أرسل /start أولاً")
            return
        step, data = _get_state(u["id"])
        if step != "awaiting_when":
            bot.answer_callback_query(c.id, "انتهت الجلسة")
            return
        choice = c.data.split(":", 1)[1]
        if choice == "now":
            _clear_state(u["id"])
            bot.answer_callback_query(c.id, "تنفيذ فوري")
            _do_execute_now(c.message.chat.id, u, data.get("app_index", -1), data.get("value", ""))
        elif choice == "sched":
            _set_state(u["id"], "schedule_delay", data)
            bot.answer_callback_query(c.id)
            bot.send_message(
                c.message.chat.id,
                "أدخل مدة التأخير (مثل: 24h أو 3d):",
                reply_markup=types.ForceReply(selective=False),
            )

    # ── أزرار المهام القديمة ──────────────────────────────────────────────────
    @bot.callback_query_handler(func=lambda c: (c.data or "").split(":")[0] in ("run", "tog", "del"))
    def h_job_cb(c):
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id, "أرسل /start أولاً")
            return
        action, _, sid = (c.data or "").partition(":")
        if not sid.isdigit():
            bot.answer_callback_query(c.id, "أمر غير معروف")
            return
        jid = int(sid)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM scheduled_jobs WHERE id=%s", (jid,))
                job = cur.fetchone()
        if not job or (job["user_id"] != u["id"] and u["role"] != "admin"):
            bot.answer_callback_query(c.id, "غير مصرّح")
            return
        if action == "run":
            execute_job.apply_async(args=[jid], countdown=0)
            bot.answer_callback_query(c.id, "▶️ أُرسلت")
        elif action == "tog":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE scheduled_jobs SET enabled = 1 - enabled WHERE id=%s", (jid,))
            bot.answer_callback_query(c.id, "تم التبديل")
        elif action == "del":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM scheduled_jobs WHERE id=%s", (jid,))
            bot.answer_callback_query(c.id, "🗑 حُذفت")

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
            if step in ("device_gaid", "device_idfa", "device_afid", "app_value") and not text:
                bot.reply_to(m, "القيمة فارغة. أرسل قيمة صحيحة:",
                             reply_markup=types.ForceReply(selective=False))
                return

            if step == "device_gaid":
                data["gaid"] = text
                _set_state(u["id"], "device_afid", data)
                bot.reply_to(m, "أرسل AFID:", reply_markup=types.ForceReply(selective=False))

            elif step == "device_idfa":
                data["idfa"] = text
                _set_state(u["id"], "device_afid", data)
                bot.reply_to(m, "أرسل AFID:", reply_markup=types.ForceReply(selective=False))

            elif step == "device_afid":
                _save_env(u["id"], os=data.get("os"), gaid=data.get("gaid", ""),
                          idfa=data.get("idfa", ""), afid=text)
                _clear_state(u["id"])
                bot.reply_to(m, "✅ تم حفظ بيانات الجهاز. (راجِعها بـ /profile)")

            elif step == "proxy":
                if text:
                    _save_env(u["id"], proxy=text)
                    msg = "✅ تم حفظ البروكسي."
                else:
                    env_now = _get_env(u["id"])
                    env_now.pop("proxy", None)
                    _ud_set(u["id"], "tg_env", json.dumps(env_now))
                    msg = "✅ بدون بروكسي (تم التعطيل)."
                _clear_state(u["id"])
                bot.reply_to(m, msg)

            elif step == "app_value":
                data["value"] = text
                _set_state(u["id"], "awaiting_when", data)
                kb = types.InlineKeyboardMarkup()
                kb.row(
                    types.InlineKeyboardButton("⚡ تنفيذ فوري", callback_data="when:now"),
                    types.InlineKeyboardButton("🗓 جدولة مخصّصة", callback_data="when:sched"),
                )
                bot.reply_to(m, "متى تريد تنفيذ هذا الاختبار؟", reply_markup=kb)

            elif step == "schedule_delay":
                minutes = _parse_delay_minutes(text)
                if minutes is None:
                    _clear_state(u["id"])
                    bot.reply_to(m, "صيغة غير صحيحة. مثال صحيح: 24h أو 3d.\nأعد /apps للبدء.")
                    return
                status, result = _schedule_test(u, data.get("app_index", -1), data.get("value", ""), minutes)
                _clear_state(u["id"])
                if status == "ok":
                    when_txt = result.strftime("%Y-%m-%d %H:%M UTC") if hasattr(result, "strftime") else str(result)
                    bot.reply_to(m, f"🗓 تمت جدولة الاختبار.\nموعد التنفيذ: {when_txt}\nخُصم من رصيدك مقدّماً.")
                elif status == "invalid":
                    bot.reply_to(m, _requirements_message(result))
                elif status == "balance":
                    bot.reply_to(m, "🚫 نفد رصيدك. تعذّرت الجدولة.")
                else:
                    bot.reply_to(m, "تعذّرت الجدولة. أعد /apps.")
            else:
                _clear_state(u["id"])
        except Exception as e:
            # تنظيف الحالة دائماً حتى لا يعلق المستخدم
            logger.error(f"[Telegram] state '{step}' error: {e}")
            try:
                _clear_state(u["id"])
            except Exception:
                pass
            bot.reply_to(m, "حدث خطأ غير متوقّع. أُعيد ضبط الجلسة — أعد المحاولة.")


_register_handlers()


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
