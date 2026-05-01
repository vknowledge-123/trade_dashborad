import json
import os
import secrets
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(
    os.getenv(
        "TRADE_DASHBOARD_DB_PATH",
        str(Path(__file__).resolve().parent / "trade_dashboard.db"),
    )
)

DEFAULT_COURSE_SETTINGS = {
    "four_month_price": 5000,
    "one_year_price": 10000,
    "support_text": "Priority support, live Q&A sessions, and direct mentorship guidance with Mentor Amol Charpe.",
}


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def _commit_with_retry(conn, attempts=3, delay=0.2):
    for attempt in range(attempts):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == attempts - 1:
                raise
            time.sleep(delay * (attempt + 1))


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            phone TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            totp_secret TEXT,
            totp_enabled INTEGER NOT NULL DEFAULT 0,
            last_login_at TEXT,
            last_login_ip TEXT,
            last_login_user_agent TEXT,
            login_count INTEGER NOT NULL DEFAULT 0,
            trial_start TEXT NOT NULL,
            trial_days INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kite_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_key TEXT NOT NULL,
            api_secret TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inquiries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            subject TEXT NOT NULL,
            message TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_login_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            ip TEXT NOT NULL,
            user_agent TEXT NOT NULL,
            success INTEGER NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market_cache (
            cache_key TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS course_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            four_month_price INTEGER NOT NULL,
            one_year_price INTEGER NOT NULL,
            support_text TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS academy_videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            youtube_url TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            is_published INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS academy_licenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assigned_email TEXT NOT NULL,
            license_key TEXT NOT NULL UNIQUE,
            plan_name TEXT NOT NULL,
            duration_days INTEGER NOT NULL,
            notes TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            starts_at TEXT,
            expires_at TEXT,
            activated_by_user_id INTEGER,
            activated_at TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(activated_by_user_id) REFERENCES users(id)
        )
        """
    )
    _commit_with_retry(conn)

    # Lightweight migration helpers for older DBs
    cur.execute("PRAGMA table_info(users)")
    cols = {row["name"] for row in cur.fetchall()}
    if "is_admin" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
    if "totp_secret" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN totp_secret TEXT")
    if "totp_enabled" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN totp_enabled INTEGER NOT NULL DEFAULT 0")
    if "last_login_at" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN last_login_at TEXT")
    if "last_login_ip" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN last_login_ip TEXT")
    if "last_login_user_agent" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN last_login_user_agent TEXT")
    if "login_count" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN login_count INTEGER NOT NULL DEFAULT 0")

    cur.execute("PRAGMA table_info(inquiries)")
    inquiry_cols = {row["name"] for row in cur.fetchall()}
    if "status" not in inquiry_cols:
        cur.execute("ALTER TABLE inquiries ADD COLUMN status TEXT NOT NULL DEFAULT 'open'")

    cur.execute("PRAGMA table_info(admin_login_audit)")
    audit_cols = {row["name"] for row in cur.fetchall()}
    if not audit_cols:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_login_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                ip TEXT NOT NULL,
                user_agent TEXT NOT NULL,
                success INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

    cur.execute("PRAGMA table_info(market_cache)")
    market_cache_cols = {row["name"] for row in cur.fetchall()}
    if not market_cache_cols:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market_cache (
                cache_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    cur.execute("PRAGMA table_info(course_settings)")
    course_settings_cols = {row["name"] for row in cur.fetchall()}
    if not course_settings_cols:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS course_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                four_month_price INTEGER NOT NULL,
                one_year_price INTEGER NOT NULL,
                support_text TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    cur.execute("PRAGMA table_info(academy_videos)")
    academy_video_cols = {row["name"] for row in cur.fetchall()}
    if not academy_video_cols:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS academy_videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                youtube_url TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0,
                is_published INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
            """
        )

    cur.execute("PRAGMA table_info(academy_licenses)")
    academy_license_cols = {row["name"] for row in cur.fetchall()}
    if not academy_license_cols:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS academy_licenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                assigned_email TEXT NOT NULL,
                license_key TEXT NOT NULL UNIQUE,
                plan_name TEXT NOT NULL,
                duration_days INTEGER NOT NULL,
                notes TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                starts_at TEXT,
                expires_at TEXT,
                activated_by_user_id INTEGER,
                activated_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(activated_by_user_id) REFERENCES users(id)
            )
            """
        )

    now = datetime.utcnow().isoformat(timespec="seconds")
    cur.execute(
        """
        INSERT INTO course_settings (id, four_month_price, one_year_price, support_text, updated_at)
        VALUES (1, ?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING
        """,
        (
            DEFAULT_COURSE_SETTINGS["four_month_price"],
            DEFAULT_COURSE_SETTINGS["one_year_price"],
            DEFAULT_COURSE_SETTINGS["support_text"],
            now,
        ),
    )

    _commit_with_retry(conn)
    conn.close()


def create_user(full_name, email, phone, password_hash, trial_days=1, is_admin=0):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat(timespec="seconds")
    cur.execute(
        """
        INSERT INTO users (full_name, email, phone, password_hash, is_admin, trial_start, trial_days, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (full_name, email.lower(), phone, password_hash, int(is_admin), now, trial_days, now),
    )
    _commit_with_retry(conn)
    user_id = cur.lastrowid
    conn.close()
    return user_id


def get_user_by_email(email):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE email = ?", (email.lower(),))
    row = cur.fetchone()
    conn.close()
    return row


def get_user_by_id(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row


def get_admin_user():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE is_admin = 1 LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return row


def delete_admin_users():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE is_admin = 1")
    _commit_with_retry(conn)
    conn.close()


def update_user_password_hash(user_id, password_hash):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET password_hash = ? WHERE id = ?", (password_hash, user_id))
    _commit_with_retry(conn)
    conn.close()


def set_admin_totp(admin_id, secret, enabled):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE users SET totp_secret = ?, totp_enabled = ? WHERE id = ?",
        (secret, int(enabled), admin_id),
    )
    _commit_with_retry(conn)
    conn.close()


def record_user_login(user_id: int, ip: str, user_agent: str):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat(timespec="seconds")
    cur.execute(
        """
        UPDATE users
        SET last_login_at = ?,
            last_login_ip = ?,
            last_login_user_agent = ?,
            login_count = COALESCE(login_count, 0) + 1
        WHERE id = ?
        """,
        (now, ip or "unknown", user_agent or "-", int(user_id)),
    )
    _commit_with_retry(conn)
    conn.close()


def get_recent_users(limit: int = 20):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM users
        WHERE is_admin = 0
        ORDER BY
          CASE WHEN last_login_at IS NULL THEN 1 ELSE 0 END,
          last_login_at DESC,
          created_at DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def log_admin_login(email, ip, user_agent, success, reason):
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        now = datetime.utcnow().isoformat(timespec="seconds")
        cur.execute(
            """
            INSERT INTO admin_login_audit (email, ip, user_agent, success, reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (email.lower(), ip, user_agent, int(success), reason, now),
        )
        _commit_with_retry(conn)
    except sqlite3.OperationalError:
        # Audit logging should not block the auth flow when SQLite is briefly busy.
        pass
    finally:
        if conn:
            conn.close()


def get_admin_login_audit(limit=20):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM admin_login_audit ORDER BY id DESC LIMIT ?",
        (int(limit),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def save_kite_credentials(api_key, api_secret):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat(timespec="seconds")
    cur.execute("DELETE FROM kite_credentials")
    cur.execute(
        """
        INSERT INTO kite_credentials (api_key, api_secret, created_at)
        VALUES (?, ?, ?)
        """,
        (api_key, api_secret, now),
    )
    _commit_with_retry(conn)
    conn.close()


def get_kite_credentials():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM kite_credentials ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return row


def create_inquiry(user_id, subject, message):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat(timespec="seconds")
    cur.execute(
        """
        INSERT INTO inquiries (user_id, subject, message, status, created_at)
        VALUES (?, ?, ?, 'open', ?)
        """,
        (user_id, subject, message, now),
    )
    _commit_with_retry(conn)
    conn.close()


def get_inquiries(limit=50):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT i.id, i.subject, i.message, i.status, i.created_at,
               u.full_name, u.email, u.phone
        FROM inquiries i
        JOIN users u ON u.id = i.user_id
        ORDER BY i.id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    conn.close()
    formatted = []
    for row in rows:
        row_dict = dict(row)
        try:
            dt = datetime.fromisoformat(row_dict.get("created_at"))
            row_dict["created_at_pretty"] = dt.strftime("%d %b %Y, %I:%M %p")
        except Exception:
            row_dict["created_at_pretty"] = row_dict.get("created_at", "")
        formatted.append(row_dict)
    return formatted


def update_inquiry_status(inquiry_id, status):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE inquiries SET status = ? WHERE id = ?", (status, int(inquiry_id)))
    _commit_with_retry(conn)
    conn.close()


def save_market_cache(cache_key: str, payload):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat(timespec="seconds")
    cur.execute(
        """
        INSERT INTO market_cache (cache_key, payload, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET
            payload = excluded.payload,
            updated_at = excluded.updated_at
        """,
        (cache_key, json.dumps(payload), now),
    )
    _commit_with_retry(conn)
    conn.close()


def load_market_cache(cache_key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT payload, updated_at FROM market_cache WHERE cache_key = ?",
        (cache_key,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    try:
        payload = json.loads(row["payload"])
    except Exception:
        return None
    payload["_cached_at"] = row["updated_at"]
    return payload


def get_course_settings():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM course_settings WHERE id = 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        return dict(DEFAULT_COURSE_SETTINGS)
    return {
        "four_month_price": int(row["four_month_price"]),
        "one_year_price": int(row["one_year_price"]),
        "support_text": row["support_text"],
        "updated_at": row["updated_at"],
    }


def update_course_settings(four_month_price, one_year_price, support_text):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat(timespec="seconds")
    cur.execute(
        """
        INSERT INTO course_settings (id, four_month_price, one_year_price, support_text, updated_at)
        VALUES (1, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            four_month_price = excluded.four_month_price,
            one_year_price = excluded.one_year_price,
            support_text = excluded.support_text,
            updated_at = excluded.updated_at
        """,
        (int(four_month_price), int(one_year_price), support_text.strip(), now),
    )
    _commit_with_retry(conn)
    conn.close()


def add_academy_video(title, youtube_url, sort_order=0, is_published=1):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat(timespec="seconds")
    cur.execute(
        """
        INSERT INTO academy_videos (title, youtube_url, sort_order, is_published, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (title.strip(), youtube_url.strip(), int(sort_order), int(is_published), now),
    )
    _commit_with_retry(conn)
    conn.close()


def delete_academy_video(video_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM academy_videos WHERE id = ?", (int(video_id),))
    _commit_with_retry(conn)
    conn.close()


def get_academy_videos(include_unpublished=False):
    conn = get_conn()
    cur = conn.cursor()
    if include_unpublished:
        cur.execute(
            """
            SELECT *
            FROM academy_videos
            ORDER BY sort_order ASC, id ASC
            """
        )
    else:
        cur.execute(
            """
            SELECT *
            FROM academy_videos
            WHERE is_published = 1
            ORDER BY sort_order ASC, id ASC
            """
        )
    rows = cur.fetchall()
    conn.close()
    return rows


def _generate_license_key():
    chunks = [
        secrets.token_hex(2).upper(),
        secrets.token_hex(2).upper(),
        secrets.token_hex(2).upper(),
        secrets.token_hex(2).upper(),
    ]
    return "IONE-" + "-".join(chunks)


def create_academy_license(assigned_email, plan_name, duration_days, notes=""):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat(timespec="seconds")
    license_key = _generate_license_key()
    while cur.execute(
        "SELECT 1 FROM academy_licenses WHERE license_key = ?",
        (license_key,),
    ).fetchone():
        license_key = _generate_license_key()
    cur.execute(
        """
        INSERT INTO academy_licenses (
            assigned_email, license_key, plan_name, duration_days, notes, status, created_at
        )
        VALUES (?, ?, ?, ?, ?, 'active', ?)
        """,
        (assigned_email.lower().strip(), license_key, plan_name.strip(), int(duration_days), notes.strip(), now),
    )
    _commit_with_retry(conn)
    license_id = cur.lastrowid
    conn.close()
    return {"id": license_id, "license_key": license_key}


def get_recent_academy_licenses(limit=50):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT l.*, u.full_name AS activated_user_name, u.email AS activated_user_email
        FROM academy_licenses l
        LEFT JOIN users u ON u.id = l.activated_by_user_id
        ORDER BY l.id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_active_license_for_user(user_id):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat(timespec="seconds")
    cur.execute(
        """
        SELECT *
        FROM academy_licenses
        WHERE activated_by_user_id = ?
          AND status = 'active'
          AND expires_at IS NOT NULL
          AND expires_at >= ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(user_id), now),
    )
    row = cur.fetchone()
    conn.close()
    return row


def activate_academy_license(user_id, user_email, license_key):
    conn = get_conn()
    cur = conn.cursor()
    normalized_key = (license_key or "").strip().upper()
    cur.execute(
        "SELECT * FROM academy_licenses WHERE license_key = ?",
        (normalized_key,),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return {"ok": False, "error": "Invalid license key."}
    if row["status"] != "active":
        conn.close()
        return {"ok": False, "error": "This license key is not active."}
    if row["assigned_email"] and row["assigned_email"].lower() != (user_email or "").lower():
        conn.close()
        return {"ok": False, "error": "This license key is assigned to a different email."}

    now = datetime.utcnow()
    expires_at = row["expires_at"]
    if row["activated_by_user_id"]:
        if int(row["activated_by_user_id"]) != int(user_id):
            conn.close()
            return {"ok": False, "error": "This license key has already been used by another user."}
        if expires_at and expires_at < now.isoformat(timespec="seconds"):
            conn.close()
            return {"ok": False, "error": "This license key has expired."}
        conn.close()
        return {"ok": True, "message": "License key already active for this account."}

    starts_at = now.isoformat(timespec="seconds")
    resolved_expires_at = (now + timedelta(days=int(row["duration_days"]))).isoformat(timespec="seconds")
    cur.execute(
        """
        UPDATE academy_licenses
        SET starts_at = ?, expires_at = ?, activated_by_user_id = ?, activated_at = ?
        WHERE id = ?
        """,
        (starts_at, resolved_expires_at, int(user_id), starts_at, int(row["id"])),
    )
    _commit_with_retry(conn)
    conn.close()
    return {"ok": True, "message": "License key activated successfully."}
