"""
web.py — Flask API Service

مسؤوليات هذا الملف فقط:
  1. استقبال الطلبات HTTP
  2. المصادقة والصلاحيات
  3. CRUD على قاعدة البيانات
  4. إرسال المهام لـ Celery Queue (وليس تنفيذها!)

لا يوجد هنا أي threading أو تنفيذ مباشر لطلبات API خارجية.
"""
import hashlib
import json
import logging
import re
import secrets
import subprocess
import sys
from datetime import datetime, timezone
from functools import wraps

import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

import config
from celery_app import celery                     # للإرسال فقط، لا للتنفيذ
from db.connection import get_conn, init_db
from tasks.job_tasks import execute_job           # استدعاء المهمة عبر Celery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = config.SECRET_KEY
CORS(app, origins="*")

SAFE_PKG_RE = re.compile(r"^[a-zA-Z0-9_\-\.]+$")


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


def get_user_from_token(token: str) -> dict | None:
    if not token:
        return None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.* FROM users u
                JOIN sessions s ON s.user_id = u.id
                WHERE s.token = %s AND u.active = 1
            """, (token,))
            row = cur.fetchone()
    return dict(row) if row else None


def check_access(user: dict) -> tuple[bool, str | None]:
    if user["role"] == "admin":
        return True, None
    if user["uses_left"] <= 0:
        return False, "Usage limit reached"
    if user["expire_at"]:
        try:
            exp = user["expire_at"]
            if isinstance(exp, str):
                exp = datetime.fromisoformat(exp)
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) > exp:
                return False, "Account expired"
        except Exception:
            pass
    return True, None


def _send_telegram_sync(token: str, chat_id: str, text: str) -> tuple[bool, str]:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=8,
        )
        return r.status_code == 200, r.text[:200]
    except Exception as e:
        return False, str(e)


def _parse_run_at(value: str) -> str:
    value = value.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(value[:19], fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    dt = datetime.fromisoformat(value.split("+")[0].split("Z")[0].strip())
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ─── Decorators ───────────────────────────────────────────────────────────────
def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("X-Token") or ""
        if request.is_json and not token:
            token = (request.json or {}).get("token", "")
        user = get_user_from_token(token)
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        request.current_user = user
        return f(*args, **kwargs)
    return decorated


def require_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("X-Token") or ""
        user  = get_user_from_token(token)
        if not user or user["role"] != "admin":
            return jsonify({"error": "Admin only"}), 403
        request.current_user = user
        return f(*args, **kwargs)
    return decorated


# ═══════════════════════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/")
def index():
    return jsonify({"status": "online", "version": "3.0"})


@app.post("/auth/login")
def login():
    data     = request.json or {}
    username = data.get("username", "").strip()
    password = data.get("password", "")
    if not username or not password:
        return jsonify({"error": "Missing credentials"}), 400

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE username=%s AND active=1", (username,))
            user = cur.fetchone()

    if not user or not secrets.compare_digest(user["password"], hash_pw(password)):
        return jsonify({"error": "Invalid credentials"}), 401

    ok, err = check_access(dict(user))
    if not ok:
        return jsonify({"error": err + " — contact admin"}), 403

    token = secrets.token_hex(32)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO sessions (token, user_id) VALUES (%s, %s)", (token, user["id"]))

    return jsonify({
        "token":      token,
        "username":   user["username"],
        "role":       user["role"],
        "uses_left":  user["uses_left"],
        "max_uses":   user["max_uses"],
        "expire_at":  user["expire_at"],
        "tg_token":   user["tg_token"]   or "",
        "tg_chat_id": user["tg_chat_id"] or "",
    })


@app.post("/auth/logout")
def logout():
    token = request.headers.get("X-Token", "")
    if token:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM sessions WHERE token=%s", (token,))
    return jsonify({"ok": True})


@app.get("/auth/me")
def me():
    token = request.headers.get("X-Token", "")
    user  = get_user_from_token(token)
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify({
        "username":   user["username"],
        "role":       user["role"],
        "uses_left":  user["uses_left"],
        "max_uses":   user["max_uses"],
        "expire_at":  user["expire_at"],
        "tg_token":   user["tg_token"]   or "",
        "tg_chat_id": user["tg_chat_id"] or "",
    })


# ═══════════════════════════════════════════════════════════════════════════════
# USER DATA
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/data")
@require_auth
def get_data():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT key, value, updated FROM user_data WHERE user_id=%s",
                (request.current_user["id"],)
            )
            rows = cur.fetchall()
    return jsonify({r["key"]: {"value": r["value"], "updated": str(r["updated"])} for r in rows})


@app.post("/data")
@require_auth
def set_data():
    data = request.json or {}
    uid  = request.current_user["id"]
    with get_conn() as conn:
        with conn.cursor() as cur:
            for key, value in data.items():
                if key == "token":
                    continue
                if len(str(key)) > 100 or len(str(value)) > 50000:
                    continue
                cur.execute("""
                    INSERT INTO user_data (user_id, key, value, updated)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (user_id, key)
                    DO UPDATE SET value=EXCLUDED.value, updated=NOW()
                """, (uid, key, str(value)))
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/settings/telegram")
@require_auth
def save_telegram():
    data = request.json or {}
    tgt  = data.get("tg_token",   "").strip() or None
    cgid = data.get("tg_chat_id", "").strip() or None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET tg_token=%s, tg_chat_id=%s WHERE id=%s",
                (tgt, cgid, request.current_user["id"])
            )
    return jsonify({"ok": True})


@app.post("/settings/telegram/test")
@require_auth
def test_telegram():
    u = request.current_user
    if not u.get("tg_token") or not u.get("tg_chat_id"):
        return jsonify({"ok": False, "error": "Not configured"}), 400
    ok, err = _send_telegram_sync(u["tg_token"], u["tg_chat_id"],
                                  "✅ *ONLINE App*\nTelegram is connected and working!")
    return jsonify({"ok": ok, "error": err if not ok else None})


# ═══════════════════════════════════════════════════════════════════════════════
# PYTHON EXECUTION
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/run")
@require_auth
def run_code():
    data = request.json or {}
    code = data.get("code", "").strip()
    if not code:
        return jsonify({"error": "No code"}), 400

    user = request.current_user
    ok, err = check_access(user)
    if not ok:
        return jsonify({"error": err}), 403

    if user["role"] != "admin":
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET uses_left=uses_left-1 WHERE id=%s", (user["id"],)
                )
    try:
        res = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True, text=True, timeout=30
        )
        return jsonify({"stdout": res.stdout, "stderr": res.stderr, "returncode": res.returncode})
    except subprocess.TimeoutExpired:
        return jsonify({"stdout": "", "stderr": "Timeout after 30s", "returncode": -1})


@app.post("/pip")
@require_auth
def pip_install():
    pkg = (request.json or {}).get("package", "").strip()
    if not pkg or not SAFE_PKG_RE.match(pkg):
        return jsonify({"error": "Invalid package name"}), 400
    try:
        res = subprocess.run(
            [sys.executable, "-m", "pip", "install", pkg, "--quiet"],
            capture_output=True, text=True, timeout=60
        )
        if res.returncode == 0:
            return jsonify({"success": True,  "message": f"{pkg} installed"})
        return jsonify({"success": False, "message": res.stderr[:500]})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# APPSFLYER PROXY (الإرسال المباشر — يخصم من رصيد المستخدم)
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/api/send-event")
@require_auth
def proxy_send_event():
    data = request.json or {}
    if not all(k in data for k in ("package", "dev_key", "body")):
        return jsonify({"success": False, "error": "Missing fields"}), 400

    user       = request.current_user
    package    = data["package"]
    dev_key    = data["dev_key"]
    body_data  = data["body"]
    event_name = body_data.get("eventName", "unknown")

    from tasks.job_tasks import _build_proxies, _log_event_history  # local import
    proxies = _build_proxies(
        data.get("proxy_host", ""), data.get("proxy_port", ""),
        data.get("proxy_user", ""), data.get("proxy_pass", ""),
    )
    try:
        response = requests.post(
            f"https://api2.appsflyer.com/inappevent/{package}",
            headers={"Content-Type": "application/json", "authentication": dev_key},
            json=body_data, proxies=proxies, timeout=15,
        )
        ok = response.status_code in (200, 201)
        _log_event_history(user["id"], package, event_name, response.status_code, ok)

        if user.get("tg_token") and user.get("tg_chat_id"):
            icon = "✅" if ok else "❌"
            _send_telegram_sync(
                user["tg_token"], user["tg_chat_id"],
                f"{icon} *Event Sent*\nGame: `{package}`\nEvent: `{event_name}`\nStatus: `{response.status_code}`"
            )
        return jsonify({"success": True, "status_code": response.status_code, "response": response.text})
    except requests.RequestException as e:
        _log_event_history(user["id"], package, event_name, 0, False)
        return jsonify({"success": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT HISTORY
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/history")
@require_auth
def get_history():
    uid   = request.current_user["id"]
    role  = request.current_user["role"]
    limit = min(int(request.args.get("limit", 200)), 500)
    ftype = request.args.get("type", "")

    with get_conn() as conn:
        with conn.cursor() as cur:
            if role == "admin" and request.args.get("all") == "1":
                q, p = "SELECT * FROM event_history WHERE 1=1", []
            else:
                q, p = "SELECT * FROM event_history WHERE user_id=%s", [uid]
            if ftype:
                q += " AND type=%s"; p.append(ftype)
            q += " ORDER BY id DESC LIMIT %s"; p.append(limit)
            cur.execute(q, p)
            rows = cur.fetchall()
    return jsonify([dict(r) for r in rows])


@app.delete("/history")
@require_auth
def clear_history():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM event_history WHERE user_id=%s", (request.current_user["id"],))
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
# JOBS (CRUD فقط — التنفيذ في worker.py)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/jobs")
@require_auth
def list_jobs():
    uid  = request.current_user["id"]
    role = request.current_user["role"]
    with get_conn() as conn:
        with conn.cursor() as cur:
            if role == "admin":
                cur.execute("SELECT * FROM scheduled_jobs ORDER BY id DESC")
            else:
                cur.execute("SELECT * FROM scheduled_jobs WHERE user_id=%s ORDER BY id DESC", (uid,))
            jobs = cur.fetchall()
    return jsonify([dict(j) for j in jobs])


@app.post("/jobs")
@require_auth
def create_job():
    data   = request.json or {}
    name   = data.get("name",   "").strip()
    events = data.get("events", [])
    run_at = data.get("run_at", "").strip()

    if not name:   return jsonify({"error": "name is required"}), 400
    if not events: return jsonify({"error": "events is required"}), 400
    if not run_at: return jsonify({"error": "run_at is required"}), 400

    try:
        run_at_norm = _parse_run_at(run_at)
    except Exception:
        return jsonify({"error": "Invalid run_at format. Use YYYY-MM-DD HH:MM:SS"}), 400

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scheduled_jobs
                    (user_id, name, events, run_at,
                     proxy_host, proxy_port, proxy_user, proxy_pass,
                     package, dev_key, gaid, afid, enabled)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1)
                RETURNING id
            """, (
                request.current_user["id"], name,
                json.dumps(events), run_at_norm,
                data.get("proxy_host", ""), data.get("proxy_port", ""),
                data.get("proxy_user", ""), data.get("proxy_pass", ""),
                data.get("package",    ""), data.get("dev_key",    ""),
                data.get("gaid",       ""), data.get("afid",       ""),
            ))
            jid = cur.fetchone()["id"]
    return jsonify({"ok": True, "id": jid})


@app.put("/jobs/<int:job_id>")
@require_auth
def update_job(job_id):
    data = request.json or {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM scheduled_jobs WHERE id=%s", (job_id,))
            job = cur.fetchone()
            if not job:
                return jsonify({"error": "Not found"}), 404
            if request.current_user["role"] != "admin" and job["user_id"] != request.current_user["id"]:
                return jsonify({"error": "Forbidden"}), 403

            raw_run_at = data.get("run_at", "").strip()
            try:
                run_at = _parse_run_at(raw_run_at) if raw_run_at else job["run_at"]
            except Exception:
                return jsonify({"error": "Invalid run_at format"}), 400

            cur.execute("""
                UPDATE scheduled_jobs SET
                    name=%s, events=%s, run_at=%s, enabled=%s,
                    proxy_host=%s, proxy_port=%s, proxy_user=%s, proxy_pass=%s,
                    package=%s, dev_key=%s, gaid=%s, afid=%s
                WHERE id=%s
            """, (
                data.get("name", job["name"]),
                json.dumps(data.get("events", job["events"])),
                run_at,
                int(data.get("enabled", job["enabled"])),
                data.get("proxy_host", job["proxy_host"] or ""),
                data.get("proxy_port", job["proxy_port"] or ""),
                data.get("proxy_user", job["proxy_user"] or ""),
                data.get("proxy_pass", job["proxy_pass"] or ""),
                data.get("package",    job["package"]    or ""),
                data.get("dev_key",    job["dev_key"]    or ""),
                data.get("gaid",       job["gaid"]       or ""),
                data.get("afid",       job["afid"]       or ""),
                job_id,
            ))
    return jsonify({"ok": True})


@app.delete("/jobs/<int:job_id>")
@require_auth
def delete_job(job_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM scheduled_jobs WHERE id=%s", (job_id,))
            job = cur.fetchone()
            if not job:
                return jsonify({"error": "Not found"}), 404
            if request.current_user["role"] != "admin" and job["user_id"] != request.current_user["id"]:
                return jsonify({"error": "Forbidden"}), 403
            cur.execute("DELETE FROM job_logs        WHERE job_id=%s",  (job_id,))
            cur.execute("DELETE FROM scheduled_jobs  WHERE id=%s",      (job_id,))
    return jsonify({"ok": True})


@app.post("/jobs/<int:job_id>/run")
@require_auth
def run_job_now(job_id):
    """
    إرسال المهمة لـ Celery Queue للتنفيذ الفوري.
    يعود بـ task_id للمتابعة لاحقاً.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM scheduled_jobs WHERE id=%s", (job_id,))
            job = cur.fetchone()

    if not job:
        return jsonify({"error": "Not found"}), 404
    if request.current_user["role"] != "admin" and job["user_id"] != request.current_user["id"]:
        return jsonify({"error": "Forbidden"}), 403

    # ← هنا الفرق الجوهري عن الكود القديم:
    # نُرسل للـ Queue بدلاً من التنفيذ المباشر
    task = execute_job.apply_async(args=[job_id])
    return jsonify({"ok": True, "task_id": task.id})


@app.get("/jobs/<int:job_id>/logs")
@require_auth
def job_logs_route(job_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM scheduled_jobs WHERE id=%s", (job_id,))
            job = cur.fetchone()
            if not job:
                return jsonify({"error": "Not found"}), 404
            if request.current_user["role"] != "admin" and job["user_id"] != request.current_user["id"]:
                return jsonify({"error": "Forbidden"}), 403
            cur.execute(
                "SELECT * FROM job_logs WHERE job_id=%s ORDER BY id DESC LIMIT 20", (job_id,)
            )
            logs = cur.fetchall()
    return jsonify([dict(l) for l in logs])


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/admin/users")
@require_admin
def admin_list_users():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id,username,role,max_uses,uses_left,expire_at,created,active,tg_token,tg_chat_id FROM users"
            )
            users = cur.fetchall()
    return jsonify([dict(u) for u in users])


@app.post("/admin/users")
@require_admin
def admin_create_user():
    data      = request.json or {}
    username  = data.get("username", "").strip()
    password  = data.get("password", "").strip()
    role      = data.get("role", "user")
    max_uses  = int(data.get("max_uses", 100))
    expire_at = data.get("expire_at") or None

    if not username or not password:
        return jsonify({"error": "username and password required"}), 400
    if role not in ("user", "admin"):
        return jsonify({"error": "Invalid role"}), 400

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users (username, password, role, max_uses, uses_left, expire_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (username, hash_pw(password), role, max_uses, max_uses, expire_at))
    except Exception:
        return jsonify({"error": "Username already exists"}), 400
    return jsonify({"ok": True})


@app.put("/admin/users/<int:uid>")
@require_admin
def admin_update_user(uid):
    data = request.json or {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id=%s", (uid,))
            user = cur.fetchone()
            if not user:
                return jsonify({"error": "Not found"}), 404

            pw        = hash_pw(data["password"]) if data.get("password") else user["password"]
            expire_at = data.get("expire_at", user["expire_at"]) or None
            role      = data.get("role", user["role"])
            if role not in ("user", "admin"):
                return jsonify({"error": "Invalid role"}), 400

            cur.execute("""
                UPDATE users SET
                    password=%s, max_uses=%s, uses_left=%s,
                    active=%s, role=%s, expire_at=%s
                WHERE id=%s
            """, (
                pw,
                int(data.get("max_uses",  user["max_uses"])),
                int(data.get("uses_left", user["uses_left"])),
                int(data.get("active",    user["active"])),
                role, expire_at, uid,
            ))
    return jsonify({"ok": True})


@app.delete("/admin/users/<int:uid>")
@require_admin
def admin_delete_user(uid):
    if uid == request.current_user["id"]:
        return jsonify({"error": "Cannot delete yourself"}), 400
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM scheduled_jobs WHERE user_id=%s", (uid,))
            jobs = cur.fetchall()
            for j in jobs:
                cur.execute("DELETE FROM job_logs WHERE job_id=%s", (j["id"],))
            cur.execute("DELETE FROM scheduled_jobs  WHERE user_id=%s", (uid,))
            cur.execute("DELETE FROM event_history   WHERE user_id=%s", (uid,))
            cur.execute("DELETE FROM user_data       WHERE user_id=%s", (uid,))
            cur.execute("DELETE FROM sessions        WHERE user_id=%s", (uid,))
            cur.execute("DELETE FROM users           WHERE id=%s",      (uid,))
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
# ERROR HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

@app.errorhandler(Exception)
def handle_exception(e):
    logger.error(f"Unhandled exception: {e}", exc_info=True)
    return jsonify({"error": "Internal Server Error"}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import os
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=config.DEBUG)
else:
    # عند تشغيل gunicorn، نُهيّئ قاعدة البيانات هنا
    init_db()
