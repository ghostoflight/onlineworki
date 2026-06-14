"""
scripts/set_webhook.py — ضبط/إزالة الـ Webhook يدوياً

الاستخدام (محلياً أو كأمر لمرة واحدة على Railway):
    python -m scripts.set_webhook set      # يسجّل العنوان
    python -m scripts.set_webhook remove   # يزيله
    python -m scripts.set_webhook info     # يعرض الحالة

يتطلّب المتغيّرات: TELEGRAM_BOT_TOKEN، و PUBLIC_BASE_URL أو RAILWAY_PUBLIC_DOMAIN،
و TELEGRAM_WEBHOOK_SECRET (اختياري لكن مُستحسَن).
"""
import sys

import telebot

import config


def _url() -> str:
    base = config.PUBLIC_BASE_URL
    if not base:
        raise SystemExit("PUBLIC_BASE_URL / RAILWAY_PUBLIC_DOMAIN غير مضبوط.")
    secret = config.TELEGRAM_WEBHOOK_SECRET or "hook"
    return f"{base.rstrip('/')}/telegram/webhook/{secret}"


def main() -> None:
    if not config.TELEGRAM_BOT_TOKEN:
        raise SystemExit("TELEGRAM_BOT_TOKEN غير مضبوط.")
    bot = telebot.TeleBot(config.TELEGRAM_BOT_TOKEN, threaded=False)
    cmd = sys.argv[1] if len(sys.argv) > 1 else "set"

    if cmd == "set":
        url = _url()
        bot.remove_webhook()
        bot.set_webhook(
            url=url,
            secret_token=(config.TELEGRAM_WEBHOOK_SECRET or None),
            drop_pending_updates=False,
        )
        print(f"✅ webhook set → {url}")
    elif cmd == "remove":
        bot.remove_webhook()
        print("🗑 webhook removed")
    elif cmd == "info":
        info = bot.get_webhook_info()
        print(f"url={info.url}\npending={info.pending_update_count}\nlast_error={info.last_error_message}")
    else:
        raise SystemExit("الأمر يجب أن يكون: set | remove | info")


if __name__ == "__main__":
    main()
