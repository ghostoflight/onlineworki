"""
bot/telegram_bot.py — بوت تلغرام مشترك تفاعلي (Webhook)

الميزات:
  • /start: تسجيل تلقائي — إن لم يكن chat_id موجوداً يُنشأ مستخدم جديد
    (role=user, max_uses=5, uses_left=5). يدعم أيضاً /start <code> لربط حساب قائم.
  • /apps: لوحة أزرار ديناميكية من games_config.GAMES_DATA.
  • حالة المحادثة (State): بعد اختيار تطبيق يطلب البوت القيمة، ويلتقط الرد عبر
    حالة مخزّنة في قاعدة البيانات (تعمل مع عدّة عمّال gunicorn — لا ذاكرة محلية).
  • التنفيذ + خصم الرصيد: فحص uses_left، خصم ذرّي، إرسال الحدث (نفس منطق
    proxy_send_event)، ثم تقرير النجاح/الفشل. إعادة الرصيد تلقائياً عند فشل الوصول.
  • /balance و /history و /status: تقرأ بناءً على tg_chat_id مباشرة.

التوافق: register_webhook(app) و maybe_setup_webhook() كما هي (يستوردهما web.py).
"""
import os
import re
import json
import hashlib
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

# قائمة التطبيقات (من ملف خارجي)
try:
    from games_config import GAMES_DATA
except Exception:
    GAMES_DATA = []
    logger.warning("[Telegram] games_config.GAMES_DATA غير موجود — /apps سيكون فارغاً")

# ─── إنشاء البوت ─────────────────────────────────────────────────────────────
bot: telebot.TeleBot | None = None
if config.TELEGRAM_BOT_TOKEN:
    bot = telebot.TeleBot(config.TELEGRAM_BOT_TOKEN, threaded=False)
    logger.info("[Telegram] bot instance created.")
else:
    logger.info("[Telegram] TELEGRAM_BOT_TOKEN not set — bot disabled.")

FREE_USES = 5  # الرصيد المجاني للمستخدم الجديد


# ═══════════════════════════════════════════════════════════════════════════════
# مساعدات قاعدة البيانات
# ═══════════════════════════════════════════════════════════════════════════════
def _user_by_chat(chat_id) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM users WHERE tg_chat_id = %s AND active = 1",
                (str(chat_id),),
            )
            return cur.fetchone()


def _auto_register(chat_id, tg_username: str | None, first_name: str | None) -> dict:
    """ينشئ مستخدماً جديداً مرتبطاً بمحادثة تلغرام برصيد مجاني."""
    raw = (tg_username or first_name or f"tg{chat_id}").strip()
    base = re.sub(r"[^\w]", "_", raw)[:40] or f"tg{chat_id}"
    pw = hashlib.sha256(os.urandom(16)).hexdigest()  # كلمة مرور عشوائية (لا تُستخدم للويب)

    candidates = [base, f"{base}_{chat_id}", f"{base}_{hashlib.sha1(os.urandom(4)).hexdigest()[:4]}"]
    for uname in candidates:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO users
                               (username, password, role, max_uses, uses_left, active, tg_chat_id)
                           VALUES (%s, %s, 'user', %s, %s, 1, %s)
                           RETURNING *""",
                        (uname, pw, FREE_USES, FREE_USES, str(chat_id)),
                    )
                    return cur.fetchone()
        except psycopg2.IntegrityError:
            continue  # تعارض اسم المستخدم — جرّب البديل التالي
    # احتمال نادر جداً: أعِد المستخدم إن أُنشئ في سباق متزامن
    existing = _user_by_chat(chat_id)
    if existing:
        return existing
    raise RuntimeError("auto-register failed")


def _consume_link_code(code: str) -> int | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id FROM user_data WHERE key = 'tg_link_code' AND value = %s",
                (code,),
            )
            row = cur.fetchone()
            if not row:
                return None
            uid = row["user_id"]
            cur.execute(
                "DELETE FROM user_data WHERE user_id = %s AND key = 'tg_link_code'",
                (uid,),
            )
            return uid


def _set_chat(user_id: int, chat_id) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET tg_chat_id = %s WHERE id = %s", (str(chat_id), user_id))


def _clear_chat(chat_id) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET tg_chat_id = NULL WHERE tg_chat_id = %s", (str(chat_id),))


# ─── حالة المحادثة (مخزّنة في user_data — تعمل مع عدّة عمّال) ─────────────────
def _set_pending(user_id: int, app_index: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO user_data (user_id, key, value, updated)
                   VALUES (%s, 'tg_pending_app', %s, NOW())
                   ON CONFLICT (user_id, key)
                   DO UPDATE SET value = EXCLUDED.value, updated = NOW()""",
                (user_id, str(app_index)),
            )


def _get_pending(user_id: int) -> int | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM user_data WHERE user_id = %s AND key = 'tg_pending_app'",
                (user_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    try:
        return int(row["value"])
    except (TypeError, ValueError):
        return None


def _clear_pending(user_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM user_data WHERE user_id = %s AND key = 'tg_pending_app'",
                (user_id,),
            )


# ─── الرصيد ──────────────────────────────────────────────────────────────────
def _consume_use(user: dict):
    """خصم ذرّي لاستخدام واحد. يعيد (نجح؟, الرصيد المتبقي)."""
    if user["role"] == "admin":
        return True, user["uses_left"]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET uses_left = uses_left - 1 WHERE id = %s AND uses_left > 0 RETURNING uses_left",
                (user["id"],),
            )
            row = cur.fetchone()
    return (True, row["uses_left"]) if row else (False, 0)


def _refund_use(user: dict) -> None:
    if user["role"] == "admin":
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET uses_left = uses_left + 1 WHERE id = %s", (user["id"],))


# ─── إرسال الحدث (نفس منطق proxy_send_event، بلا استيراد دائري) ───────────────
def _dispatch_event(app_cfg: dict, value: str, user: dict):
    """
    يرسل حدث AppsFlyer لتطبيق معيّن.
    يعيد (ok, info, transport_error) — transport_error=True يعني لم يصل الطلب.
    """
    package = app_cfg["package"]
    dev_key = (
        app_cfg.get("dev_key")
        or getattr(config, "DEFAULT_DEV_KEY", "")
        or os.environ.get("DEFAULT_DEV_KEY", "")
    )
    event_name = app_cfg.get("event", "af_level_achieved")
    body = {
        "appsflyer_id":   app_cfg.get("afid", ""),
        "advertising_id": app_cfg.get("gaid", ""),
        "eventName":      event_name,
        "eventTime":      datetime.now(timezone.utc).isoformat(),
        "eventValue":     json.dumps({"value": value}),
    }
    proxies = _build_proxies(
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


# ═══════════════════════════════════════════════════════════════════════════════
# Decorator: أوامر تتطلّب حساباً
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
def _register_handlers():
    if not bot:
        return

    # ── /start: ربط أو تسجيل تلقائي ──────────────────────────────────────────
    @bot.message_handler(commands=["start"])
    def h_start(m):
        parts = (m.text or "").split(maxsplit=1)
        u = _user_by_chat(m.chat.id)

        # ربط حساب قائم عبر رمز (اختياري — يبقي تدفّق الويب يعمل)
        if not u and len(parts) == 2:
            uid = _consume_link_code(parts[1].strip().upper())
            if uid:
                _set_chat(uid, m.chat.id)
                u = _user_by_chat(m.chat.id)
                bot.reply_to(m, f"✅ تم ربط حسابك {u['username']}.\nاكتب /apps للبدء.")
                return

        if u:
            bot.reply_to(
                m,
                f"مرحباً {u['username']} 👋\n"
                f"رصيدك: {u['uses_left']}/{u['max_uses']}\n"
                f"اكتب /apps لعرض التطبيقات.",
            )
            return

        # تسجيل تلقائي
        u = _auto_register(m.chat.id, m.from_user.username, m.from_user.first_name)
        bot.reply_to(
            m,
            f"أهلاً {u['username']} 👋\n"
            f"تم إنشاء حسابك تلقائياً.\n"
            f"🎁 رصيدك المجاني: {u['uses_left']} عمليات.\n\n"
            f"اكتب /apps لاختيار تطبيق والبدء.",
        )

    @bot.message_handler(commands=["help"])
    def h_help(m):
        bot.reply_to(
            m,
            "الأوامر:\n"
            "/apps — التطبيقات المتاحة\n"
            "/balance — رصيدك\n"
            "/history — آخر العمليات\n"
            "/status — حالتك\n"
            "/unlink — فكّ الربط",
        )

    @bot.message_handler(commands=["unlink"])
    def h_unlink(m):
        _clear_chat(m.chat.id)
        bot.reply_to(m, "تم فكّ ربط هذا الحساب. أرسل /start للبدء مجدداً.")

    @bot.message_handler(commands=["balance"])
    @linked
    def h_balance(m, u):
        bot.reply_to(m, f"💳 رصيدك: {u['uses_left']} من {u['max_uses']}")

    @bot.message_handler(commands=["status"])
    @linked
    def h_status(m, u):
        bot.reply_to(
            m,
            f"👤 {u['username']}\nالدور: {u['role']}\nالرصيد: {u['uses_left']}/{u['max_uses']}",
        )

    @bot.message_handler(commands=["history"])
    @linked
    def h_history(m, u):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT event_name, status, ok, created_at
                       FROM event_history WHERE user_id = %s
                       ORDER BY id DESC LIMIT 8""",
                    (u["id"],),
                )
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

    # ── /apps: لوحة ديناميكية ────────────────────────────────────────────────
    @bot.message_handler(commands=["apps"])
    @linked
    def h_apps(m, u):
        if not GAMES_DATA:
            bot.reply_to(m, "لا توجد تطبيقات مُعرّفة بعد.")
            return
        kb = types.InlineKeyboardMarkup()
        row = []
        for i, app_cfg in enumerate(GAMES_DATA):
            row.append(types.InlineKeyboardButton(app_cfg["name"], callback_data=f"app:{i}"))
            if len(row) == 2:
                kb.row(*row)
                row = []
        if row:
            kb.row(*row)
        bot.send_message(m.chat.id, "اختر التطبيق:", reply_markup=kb)

    # ── اختيار تطبيق → طلب القيمة (حالة) ─────────────────────────────────────
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
        _set_pending(u["id"], idx)
        bot.answer_callback_query(c.id)
        bot.send_message(
            c.message.chat.id,
            f"📲 {GAMES_DATA[idx]['name']}\nيرجى إرسال رقم المستوى/القيمة المطلوبة:",
            reply_markup=types.ForceReply(selective=False),
        )

    # ── أزرار المهام القديمة (إن وُجدت) ──────────────────────────────────────
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
                cur.execute("SELECT * FROM scheduled_jobs WHERE id = %s", (jid,))
                job = cur.fetchone()
        if not job or (job["user_id"] != u["id"] and u["role"] != "admin"):
            bot.answer_callback_query(c.id, "غير مصرّح")
            return
        if action == "run":
            execute_job.apply_async(args=[jid], countdown=0)
            bot.answer_callback_query(c.id, "▶️ أُرسلت للتنفيذ")
        elif action == "tog":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE scheduled_jobs SET enabled = 1 - enabled WHERE id = %s", (jid,))
            bot.answer_callback_query(c.id, "تم التبديل")
        elif action == "del":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM scheduled_jobs WHERE id = %s", (jid,))
            bot.answer_callback_query(c.id, "🗑 حُذفت")

    # ── التقاط القيمة (حالة محادثة عبر DB) ───────────────────────────────────
    def _has_pending(chat_id) -> bool:
        u = _user_by_chat(chat_id)
        return bool(u) and _get_pending(u["id"]) is not None

    @bot.message_handler(
        func=lambda m: bool(m.text) and not m.text.startswith("/") and _has_pending(m.chat.id),
        content_types=["text"],
    )
    def h_value(m):
        u = _user_by_chat(m.chat.id)
        if not u:
            return
        idx = _get_pending(u["id"])
        _clear_pending(u["id"])
        if idx is None or idx < 0 or idx >= len(GAMES_DATA):
            bot.reply_to(m, "انتهت الجلسة. أعد /apps.")
            return

        app_cfg = GAMES_DATA[idx]
        value = m.text.strip()

        # فحص + خصم الرصيد (ذرّي)
        ok_bal, left = _consume_use(u)
        if not ok_bal:
            bot.reply_to(m, "🚫 عذراً، نفد رصيدك. لا يمكن تنفيذ العملية.")
            return

        bot.send_chat_action(m.chat.id, "typing")
        ok, info, transport_err = _dispatch_event(app_cfg, value, u)

        if not ok and transport_err:
            _refund_use(u)          # لم يصل الطلب — نعيد الرصيد
            left += 1

        if ok:
            bot.reply_to(
                m,
                f"✅ تم الإرسال بنجاح.\n"
                f"التطبيق: {app_cfg['name']}\nالقيمة: {value}\n"
                f"الرصيد المتبقي: {left}",
            )
        else:
            bot.reply_to(
                m,
                f"❌ فشل الإرسال ({info}).\nالرصيد المتبقي: {left}",
            )


_register_handlers()


# ═══════════════════════════════════════════════════════════════════════════════
# تكامل Flask (Webhook) — يستوردها web.py
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
        bot.set_webhook(
            url=url,
            secret_token=(config.TELEGRAM_WEBHOOK_SECRET or None),
            drop_pending_updates=False,
        )
        logger.info(f"[Telegram] webhook set → {url}")
    except Exception as e:
        logger.warning(f"[Telegram] setWebhook failed: {e}")
