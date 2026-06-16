"""
locales.py — طبقة الترجمة (i18n)

نقي تماماً: لا يتصل بقاعدة البيانات. المتحكّم (telegram_bot.py) يقرأ لغة
المستخدم من قاعدة البيانات ثم يستدعي lookup(key, lang).

الاستخدام من المتحكّم:
    from locales import lookup, SUPPORTED, DEFAULT_LANG
    text = lookup("welcome", lang, name="Zen")
"""

DEFAULT_LANG = "ar"
SUPPORTED = ("ar", "en", "bn")

LOCALES = {
    "ar": {
        # عام
        "welcome":        "أهلاً {name} 👋\nتم تجهيز حسابك.\n🎁 رصيدك: {credits} عمليات.",
        "welcome_back":   "مرحباً {name} 👋\nالرصيد: {credits}/{max}",
        "main_menu":      "القائمة الرئيسية — اختر:",
        "reset_done":     "♻️ تمت إعادة الضبط. عدنا إلى القائمة الرئيسية.",
        "session_over":   "انتهت الجلسة. ابدأ من جديد عبر /apps.",
        "unexpected":     "حدث خطأ غير متوقّع. أُعيد ضبط الجلسة — أعد المحاولة.",
        "need_start":     "أرسل /start أولاً لإنشاء حسابك.",
        # أزرار عامة
        "btn_new_task":   "➕ مهمة جديدة",
        "btn_apps":       "🧪 التطبيقات",
        "btn_profile":    "👤 حسابي",
        "btn_settings":   "⚙️ الإعدادات",
        "btn_balance":    "📊 رصيدي",
        "btn_services":   "💳 الخدمات",
        "btn_back":       "🔙 رجوع",
        "btn_cancel":     "❌ إلغاء",
        # تدفّق متسلسل (OS → فئة → تطبيق)
        "choose_os":       "اختر نظام تشغيل جهاز الاختبار:",
        "choose_category": "اختر الفئة:",
        "choose_game":     "اختر التطبيق:",
        "no_games":        "لا توجد تطبيقات في هذه الفئة.",
        # إدخال بيانات المهمة (معزولة لكل مهمة)
        "ask_gaid":  "أرسل <b>GAID</b> الخاص بهذه المهمة:",
        "ask_idfa":  "أرسل <b>IDFA</b> الخاص بهذه المهمة:",
        "ask_afid":  "أرسل <b>AFID</b> الخاص بهذه المهمة:",
        "ask_event": "أرسل <b>اسم الحدث</b>:",
        "ask_delay": "أرسل صيغة <code>Event | Hours</code> (مثل <code>Purchase | 24</code>):",
        "bad_value": "⚠️ قيمة غير صالحة. أعد الإرسال:",
        # الرصيد/الاشتراك
        "no_credits":     "🚫 لا يوجد رصيد كافٍ أو انتهى اشتراكك. اشترك للمتابعة عبر 💳 الخدمات.",
        "exec_ok":        "✅ تم التنفيذ.\nالحدث: <code>{event}</code>\nالرصيد: {left}",
        "exec_fail":      "❌ فشل التنفيذ ({info}).\nالرصيد: {left}",
        "sched_ok":       "🗓 تمت الجدولة.\nالحدث: <code>{event}</code>\nالتنفيذ: <code>{when}</code>",
        # الخدمات (مرحلة لاحقة)
        "services_menu":  "💳 الخدمات — اختر:",
        "btn_subscribe":  "💎 الاشتراك/الدفع",
        "btn_sub_status": "📅 حالة الاشتراك",
        "btn_support":    "🆘 الدعم",
        "lang_choose":    "اختر اللغة:",
        "lang_set":       "✅ تم ضبط اللغة: العربية",
        # الاشتراك والدفع
        "choose_plan":    "اختر الباقة:",
        "pay_instructions": "💎 باقة <b>{plan}</b> — {credits} عملية لمدة {days} يوماً.\n\nحوّل قيمة الاشتراك ثم أرسل <b>لقطة شاشة</b> للتحويل كصورة هنا للمراجعة اليدوية.",
        "pay_received":   "✅ تم استلام طلبك وهو قيد المراجعة. سنُعلمك عند التفعيل.",
        "pay_need_photo": "أرسل لقطة الشاشة كصورة (وليس ملفاً).",
        "pay_approved":   "🎉 تم تفعيل اشتراكك <b>{plan}</b> حتى <code>{until}</code>.\nأُضيف {credits} إلى رصيدك.",
        "pay_rejected":   "❌ لم يُقبل إثبات الدفع. لمزيد من المساعدة استخدم 🆘 الدعم.",
        "sub_active":     "📅 اشتراكك فعّال حتى <code>{until}</code>.\nالرصيد: {credits}.",
        "sub_none":       "لا يوجد اشتراك فعّال حالياً.\nالرصيد: {credits}.",
        "support_prompt": "اكتب رسالتك للدعم وسنتواصل معك:",
        "support_sent":   "✅ تم إرسال رسالتك للدعم.",
        # المهام والتحرير
        "jobs_title":     "📋 مهامك المجدولة:",
        "jobs_empty":     "لا توجد مهام مجدولة.",
        "edit_menu":      "✏️ اختر الحقل لتعديله:",
        "edit_prompt":    "القيمة الحالية:\n<code>{cur}</code>\n\nأرسل القيمة الجديدة:",
        "edit_done":      "✅ تم تحديث المهمة.",
        "btn_run":        "▶️ تشغيل الآن",
        "btn_toggle":     "⏸️ تفعيل/إيقاف",
        "btn_edit":       "✏️ تعديل",
        "btn_delete":     "🗑️ حذف",
        "btn_f_name":     "📌 الاسم",
        "btn_f_devkey":   "🔑 Dev Key",
        "btn_f_events":   "📡 الأحداث",
    },
    "en": {
        "welcome":        "Welcome {name} 👋\nYour account is ready.\n🎁 Credits: {credits}.",
        "welcome_back":   "Hi {name} 👋\nCredits: {credits}/{max}",
        "main_menu":      "Main menu — choose:",
        "reset_done":     "♻️ Reset done. Back to the main menu.",
        "session_over":   "Session ended. Start again with /apps.",
        "unexpected":     "Unexpected error. Session reset — please retry.",
        "need_start":     "Send /start first to create your account.",
        "btn_new_task":   "➕ New task",
        "btn_apps":       "🧪 Apps",
        "btn_profile":    "👤 Profile",
        "btn_settings":   "⚙️ Settings",
        "btn_balance":    "📊 Credits",
        "btn_services":   "💳 Services",
        "btn_back":       "🔙 Back",
        "btn_cancel":     "❌ Cancel",
        "choose_os":       "Choose the test device OS:",
        "choose_category": "Choose a category:",
        "choose_game":     "Choose an app:",
        "no_games":        "No apps in this category.",
        "ask_gaid":  "Send the <b>GAID</b> for this task:",
        "ask_idfa":  "Send the <b>IDFA</b> for this task:",
        "ask_afid":  "Send the <b>AFID</b> for this task:",
        "ask_event": "Send the <b>event name</b>:",
        "ask_delay": "Send <code>Event | Hours</code> (e.g. <code>Purchase | 24</code>):",
        "bad_value": "⚠️ Invalid value. Send again:",
        "no_credits":     "🚫 Not enough credits or your subscription expired. Subscribe via 💳 Services.",
        "exec_ok":        "✅ Executed.\nEvent: <code>{event}</code>\nCredits: {left}",
        "exec_fail":      "❌ Execution failed ({info}).\nCredits: {left}",
        "sched_ok":       "🗓 Scheduled.\nEvent: <code>{event}</code>\nRun at: <code>{when}</code>",
        "services_menu":  "💳 Services — choose:",
        "btn_subscribe":  "💎 Subscribe/Pay",
        "btn_sub_status": "📅 Subscription status",
        "btn_support":    "🆘 Support",
        "lang_choose":    "Choose language:",
        "lang_set":       "✅ Language set: English",
        "choose_plan":    "Choose a plan:",
        "pay_instructions": "💎 <b>{plan}</b> plan — {credits} ops for {days} days.\n\nTransfer the fee, then send a <b>screenshot</b> of the transfer as a photo here for manual review.",
        "pay_received":   "✅ Your request was received and is under review. We'll notify you on activation.",
        "pay_need_photo": "Send the screenshot as a photo (not a file).",
        "pay_approved":   "🎉 Your <b>{plan}</b> subscription is active until <code>{until}</code>.\n{credits} credits added.",
        "pay_rejected":   "❌ Payment proof was not accepted. Use 🆘 Support for help.",
        "sub_active":     "📅 Subscription active until <code>{until}</code>.\nCredits: {credits}.",
        "sub_none":       "No active subscription.\nCredits: {credits}.",
        "support_prompt": "Type your support message and we'll get back to you:",
        "support_sent":   "✅ Your message was sent to support.",
        "jobs_title":     "📋 Your scheduled tasks:",
        "jobs_empty":     "No scheduled tasks.",
        "edit_menu":      "✏️ Choose a field to edit:",
        "edit_prompt":    "Current value:\n<code>{cur}</code>\n\nSend the new value:",
        "edit_done":      "✅ Task updated.",
        "btn_run":        "▶️ Run now",
        "btn_toggle":     "⏸️ Enable/Disable",
        "btn_edit":       "✏️ Edit",
        "btn_delete":     "🗑️ Delete",
        "btn_f_name":     "📌 Name",
        "btn_f_devkey":   "🔑 Dev Key",
        "btn_f_events":   "📡 Events",
    },
}


def lookup(key, lang=DEFAULT_LANG, **kwargs):
    """يعيد النص المترجَم؛ يرجع للعربية ثم للمفتاح نفسه عند الغياب."""
    table = LOCALES.get(lang) or LOCALES[DEFAULT_LANG]
    s = table.get(key)
    if s is None:
        s = LOCALES[DEFAULT_LANG].get(key, key)
    if kwargs:
        try:
            s = s.format(**kwargs)
        except (KeyError, IndexError, ValueError):
            pass
    return s
