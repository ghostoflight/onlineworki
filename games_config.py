"""
games_config.py — قائمة التطبيقات المتاحة للبوت

استبدل هذه القائمة الوهمية بقائمتك الحقيقية.
الحقول الإضافية (اختيارية) لكل تطبيق لتفعيل الإرسال الفعلي:
    dev_key, gaid, afid, event, proxy_host, proxy_port, proxy_user, proxy_pass
إن غابت، يُستخدم DEFAULT_DEV_KEY من البيئة (إن وُجد) وإلا يُعدّ الإرسال محاكاةً.
"""

GAMES_DATA = [
    {"name": "App 1", "package": "com.example.app1", "cat": "puzzle"},
    {"name": "App 2", "package": "com.example.app2", "cat": "other"},
    {"name": "App 3", "package": "com.example.app3", "cat": "action"},
]
