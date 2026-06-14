# التعديلات (مشروع مُصلَح)

## إصلاحات الأخطاء
1. **سباق الإرسال المزدوج** — `tasks/job_tasks.py: scan_and_dispatch_due_jobs`
   صار يطالب بالمهام المستحقة عبر `UPDATE ... RETURNING` ذرّية بدل
   `SELECT` ثم تعطيل لاحق، فلا تلتقط دورتا Beat نفس المهمة وترسلها مرتين.

2. **تكرار إرسال الأحداث عند Retry** — `tasks/job_tasks.py: execute_job`
   كان خطأ الاتصال يُعيد تشغيل المهمة كاملةً فيُعيد إرسال الأحداث الناجحة.
   الآن: إعادة التشغيل مسموحة فقط *قبل* إرسال أي حدث؛ بعد ذلك يُسجَّل الفشل
   ويُكمل دون تكرار. مع backoff أُسّي حقيقي.

3. **بيانات اعتماد البروكسي** — `_build_proxies` صار يرمّز المستخدم/كلمة المرور
   (URL-encode) فلا يكسر الرابط وجود `@ : /` فيها.

4. **تهيئة Connection Pool** — `db/connection.py: get_pool`
   double-checked locking يمنع إنشاء أكثر من pool عند أول طلبين متزامنين.

## ملفات ناقصة أُضيفت
- `tasks/__init__.py`, `db/__init__.py` — ضرورية لعمل الـ package imports.
- `.gitignore`, `.env.example` — ملفات مساعدة.

## ضبط بناء Railway (مهم)
- `requirements.txt`: رُفِع `psycopg2-binary` إلى **2.9.10** — يوفّر wheels
  جاهزة لبايثون 3.11/3.12/3.13 بلا حاجة لأدوات بناء (يمنع فشل التجميع).
- `runtime.txt`: تثبيت **python-3.12.7** — متوافق مع celery 5.4 و psycopg2،
  ويمنع اختيار 3.13 (غير مدعوم رسمياً في celery 5.4).
- إن اعترض Railway على سطر إصدار بايثون لأي سبب: احذف `runtime.txt`؛
  ترقية psycopg2 إلى 2.9.10 وحدها تكفي لتجنّب التجميع.

## لم يُغيَّر (سلوكياً)
`web.py`, `worker.py`, `celery_app.py`, `config.py`, `Procfile` — كما هي.
أسماء الدوال المستوردة في `web.py` محفوظة: `execute_job`,
`_build_proxies`, `_log_event_history`.
