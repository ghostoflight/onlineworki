# بوت تلغرام (مشترك · تفاعلي · Webhook)

بوت واحد يكلّمه كل المستخدمين، يعيد استخدام نفس قاعدة البيانات و Celery.
لا يحتاج خدمة Railway رابعة — يعمل داخل خدمة `web` الحالية عبر Webhook.

## ١) إنشاء البوت
1. افتح **@BotFather** → `/newbot` → خذ **التوكن** واسم المستخدم.

## ٢) متغيّرات البيئة (في خدمة web على Railway)
```
TELEGRAM_BOT_TOKEN=<التوكن من BotFather>
TELEGRAM_BOT_USERNAME=<اسم البوت بدون @>
TELEGRAM_WEBHOOK_SECRET=<نص عشوائي طويل>
```
`PUBLIC_BASE_URL` يُشتق تلقائياً على Railway من `RAILWAY_PUBLIC_DOMAIN`.
لو لم يُضبط تلقائياً عيّنه يدوياً: `PUBLIC_BASE_URL=https://your-app.up.railway.app`

## ٣) تسجيل الـ Webhook
يحدث **تلقائياً** عند إقلاع خدمة web (إن توفّرت المتغيّرات أعلاه).
أو يدوياً لمرة واحدة:
```
python -m scripts.set_webhook set     # تسجيل
python -m scripts.set_webhook info    # عرض الحالة
python -m scripts.set_webhook remove  # إزالة
```

## ٤) ربط حساب مستخدم بالبوت
1. من التطبيق (مستخدم مسجّل دخوله) استدعِ:
   `POST /api/telegram/link-code` (مع ترويسة `X-Token`)
   يُرجِع `{ "code": "AB12CD", "deep_link": "https://t.me/YourAppBot?start=AB12CD" }`
2. المستخدم يفتح الرابط أو يرسل للبوت: `/start AB12CD`
3. يُربط `chat_id` بحسابه. الرمز يُستهلَك لمرة واحدة.
   لفكّ الربط: `/unlink` أو `POST /api/telegram/unlink`.

## أوامر البوت
`/start <code>` ربط · `/help` · `/status` · `/balance` ·
`/jobs` (أزرار: ▶️ تشغيل / ⏸ تبديل / 🗑 حذف) · `/history` · `/unlink`

## الأمان
- مسار الـ webhook يحوي السرّ، ويُتحقّق إضافياً من ترويسة
  `X-Telegram-Bot-Api-Secret-Token`.
- كل أمر يتطلّب حساباً مربوطاً؛ أزرار المهام تتحقّق أن المهمة تخصّ المستخدم
  (أو أنه admin).

## ملاحظة على البناء (Railway)
`pyTelegramBotAPI` مكتبة Python خالصة — لا تتطلّب أدوات بناء، فلا تؤثّر على
نجاح build على Railway.

## التوافق مع الإشعارات القديمة
`execute_job` صار يرسل الإشعار عبر البوت المشترك (`TELEGRAM_BOT_TOKEN`)،
ويرجع لتوكن المستخدم القديم (`tg_token`) فقط إن لم يُضبط البوت المشترك.
