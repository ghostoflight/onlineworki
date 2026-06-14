"""
bot/telegram_bot.py — بوت تلغرام مشترك تفاعلي (Webhook)

يعيد استخدام نفس الـ backend:
  • قاعدة البيانات عبر db.connection.get_conn
  • تنفيذ المهام عبر tasks.job_tasks.execute_job (Celery)

نموذج التشغيل:
  • بوت واحد مشترك (توكنه في TELEGRAM_BOT_TOKEN).
  • التوصيل Webhook: register_webhook(app) يضيف المسار إلى Flask،
    و maybe_setup_webhook() يسجّل العنوان لدى تلغرام عند الإقلاع.
  • الربط الآمن: المستخدم يحصل على رمز من التطبيق ثم يرسل /start <code>.

ملاحظة: parse_mode = None (نص عادي) لتجنّب كسر Markdown بأسماء المهام.
"""
import logging
from functools import wraps

import telebot
from telebot import types
from flask import request

import config
from db.connection import get_conn
from tasks.job_tasks import execute_job

logger = logging.getLogger(__name__)

# ─── إنشاء البوت (فقط إن وُجد التوكن) ────────────────────────────────────────
bot: telebot.TeleBot | None = None
if config.TELEGRAM_BOT_TOKEN:
    bot = telebot.TeleBot(config.TELEGRAM_BOT_TOKEN, threaded=False)
    logger.info("[Telegram] bot instance created.")
else:
    logger.info("[Telegram] TELEGRAM_BOT_TOKEN not set — bot disabled.")


# ═══════════════════════════════════════════════════════════════════════════════
# DB Helpers
# ═══════════════════════════════════════════════════════════════════════════════
def _user_by_chat(chat_id) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM users WHERE tg_chat_id = %s AND active = 1",
                (str(chat_id),),
            )
            return cur.fetchone()


def _consume_link_code(code: str) -> int | None:
    """يتحقّق من رمز الربط ويستهلكه (لمرة واحدة)، ويعيد user_id."""
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
            cur.execute(
                "UPDATE users SET tg_chat_id = %s WHERE id = %s",
                (str(chat_id), user_id),
            )


def _clear_chat(chat_id) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET tg_chat_id = NULL WHERE tg_chat_id = %s",
                (str(chat_id),),
            )


def _job_owned(job_id: int, user: dict) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM scheduled_jobs WHERE id = %s", (job_id,))
            job = cur.fetchone()
    if not job:
        return None
    if job["user_id"] != user["id"] and user["role"] != "admin":
        return None
    return job


# ═══════════════════════════════════════════════════════════════════════════════
# Decorator: أوامر تتطلّب حساباً مربوطاً
# ═══════════════════════════════════════════════════════════════════════════════
def linked(handler):
    @wraps(handler)
    def wrap(message):
        user = _user_by_chat(message.chat.id)
        if not user:
            bot.reply_to(
                message,
                "🔒 حسابك غير مربوط.\n"
                "من التطبيق: الإعدادات ← احصل على رمز الربط، ثم أرسل:\n"
                "/start الرمز",
            )
            return
        return handler(message, user)
    return wrap


# ═══════════════════════════════════════════════════════════════════════════════
# Handlers — تُسجَّل فقط إن كان البوت مُفعّلاً
# ═══════════════════════════════════════════════════════════════════════════════
def _register_handlers():
    if not bot:
        return

    @bot.message_handler(commands=["start"])
    def h_start(m):
        parts = (m.text or "").split(maxsplit=1)
        if len(parts) == 2:
            code = parts[1].strip().upper()
            uid = _consume_link_code(code)
            if uid:
                _set_chat(uid, m.chat.id)
                bot.reply_to(m, "✅ تم ربط حسابك بنجاح. أرسل /help لعرض الأوامر.")
            else:
                bot.reply_to(m, "⚠️ رمز ربط غير صالح أو مُستهلَك. أنشئ رمزاً جديداً من التطبيق.")
            return
        u = _user_by_chat(m.chat.id)
        if u:
            bot.reply_to(m, f"مرحباً {u['username']} 👋\nأرسل /help لعرض الأوامر.")
        else:
            bot.reply_to(
                m,
                "مرحباً! 👋\n"
                "لربط حسابك: من التطبيق ← الإعدادات ← احصل على رمز الربط، ثم أرسل:\n"
                "/start الرمز",
            )

    @bot.message_handler(commands=["help"])
    def h_help(m):
        bot.reply_to(
            m,
            "الأوامر المتاحة:\n"
            "/status — حالة سريعة\n"
            "/balance — المتبقي من الحصة\n"
            "/jobs — مهامك المجدولة (مع أزرار)\n"
            "/history — آخر النتائج\n"
            "/unlink — فكّ ربط هذا الحساب",
        )

    @bot.message_handler(commands=["unlink"])
    def h_unlink(m):
        _clear_chat(m.chat.id)
        bot.reply_to(m, "تم فكّ ربط هذا الحساب. أرسل /start <code> للربط مجدداً.")

    @bot.message_handler(commands=["status"])
    @linked
    def h_status(m, u):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS c FROM scheduled_jobs WHERE user_id = %s AND enabled = 1",
                    (u["id"],),
                )
                active = cur.fetchone()["c"]
        bot.reply_to(
            m,
            f"👤 {u['username']}\n"
            f"الدور: {u['role']}\n"
            f"الحصة المتبقية: {u['uses_left']}/{u['max_uses']}\n"
            f"المهام النشطة: {active}",
        )

    @bot.message_handler(commands=["balance"])
    @linked
    def h_balance(m, u):
        bot.reply_to(m, f"💳 المتبقي: {u['uses_left']} من {u['max_uses']}")

    @bot.message_handler(commands=["jobs"])
    @linked
    def h_jobs(m, u):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT id, name, enabled, last_status
                       FROM scheduled_jobs WHERE user_id = %s
                       ORDER BY id DESC LIMIT 10""",
                    (u["id"],),
                )
                jobs = cur.fetchall()
        if not jobs:
            bot.reply_to(m, "لا توجد مهام مجدولة.")
            return
        for j in jobs:
            state = "🟢 مفعّلة" if j["enabled"] else "⚪️ موقوفة"
            status = j["last_status"] or "—"
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("▶️ تشغيل", callback_data=f"run:{j['id']}"),
                types.InlineKeyboardButton(
                    "⏸ إيقاف" if j["enabled"] else "▶️ تفعيل",
                    callback_data=f"tog:{j['id']}",
                ),
                types.InlineKeyboardButton("🗑 حذف", callback_data=f"del:{j['id']}"),
            )
            bot.send_message(
                m.chat.id,
                f"#{j['id']} — {j['name']}\n{state} · آخر حالة: {status}",
                reply_markup=kb,
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
        bot.reply_to(m, "آخر النتائج:\n" + "\n".join(lines))

    @bot.callback_query_handler(func=lambda c: True)
    def h_callback(c):
        u = _user_by_chat(c.message.chat.id)
        if not u:
            bot.answer_callback_query(c.id, "اربط حسابك أولاً")
            return
        action, _, sid = (c.data or "").partition(":")
        if not sid.isdigit():
            bot.answer_callback_query(c.id, "أمر غير معروف")
            return
        jid = int(sid)
        job = _job_owned(jid, u)
        if not job:
            bot.answer_callback_query(c.id, "غير مصرّح")
            return

        if action == "run":
            execute_job.apply_async(args=[jid], countdown=0)
            bot.answer_callback_query(c.id, "▶️ أُرسلت للتنفيذ")
        elif action == "tog":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE scheduled_jobs SET enabled = 1 - enabled WHERE id = %s",
                        (jid,),
                    )
            bot.answer_callback_query(c.id, "تم التبديل")
        elif action == "del":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM scheduled_jobs WHERE id = %s", (jid,))
            bot.answer_callback_query(c.id, "🗑 حُذفت")
            try:
                bot.edit_message_text(
                    f"#{jid} — حُذفت.", c.message.chat.id, c.message.message_id
                )
            except Exception:
                pass
        else:
            bot.answer_callback_query(c.id, "أمر غير معروف")


_register_handlers()


# ═══════════════════════════════════════════════════════════════════════════════
# Flask integration
# ═══════════════════════════════════════════════════════════════════════════════
def register_webhook(app) -> None:
    """يضيف مسار الـ webhook إلى تطبيق Flask (يُستدعى من web.py)."""
    if not bot:
        logger.info("[Telegram] webhook route not added (bot disabled).")
        return

    secret = config.TELEGRAM_WEBHOOK_SECRET or "hook"
    path = f"/telegram/webhook/{secret}"

    def _webhook():
        # دفاع متعدّد الطبقات: تحقّق من ترويسة سرّ تلغرام
        if config.TELEGRAM_WEBHOOK_SECRET:
            hdr = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if hdr != config.TELEGRAM_WEBHOOK_SECRET:
                return ("forbidden", 403)
        if request.headers.get("content-type", "").startswith("application/json"):
            raw = request.get_data().decode("utf-8")
            update = types.Update.de_json(raw)
            bot.process_new_updates([update])
            return ("", 200)
        return ("bad request", 400)

    app.add_url_rule(path, "telegram_webhook", _webhook, methods=["POST"])
    logger.info(f"[Telegram] webhook route registered: {path}")


def maybe_setup_webhook() -> None:
    """يسجّل عنوان الـ webhook لدى تلغرام تلقائياً إن توفّر العنوان العام."""
    if not bot:
        return
    base = config.PUBLIC_BASE_URL
    if not base:
        logger.info(
            "[Telegram] PUBLIC_BASE_URL/RAILWAY_PUBLIC_DOMAIN غير مضبوط — "
            "سجّل الـ webhook يدوياً عبر scripts/set_webhook.py"
        )
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
