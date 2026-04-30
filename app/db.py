import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).resolve().parent / "trade_dashboard.db"


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
    conn.commit()

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

    conn.commit()
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
    conn.commit()
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
    conn.commit()
    conn.close()


def update_user_password_hash(user_id, password_hash):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET password_hash = ? WHERE id = ?", (password_hash, user_id))
    conn.commit()
    conn.close()


def set_admin_totp(admin_id, secret, enabled):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE users SET totp_secret = ?, totp_enabled = ? WHERE id = ?",
        (secret, int(enabled), admin_id),
    )
    conn.commit()
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
    conn.commit()
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
    conn.commit()
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
    conn.commit()
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
    conn.commit()
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
    # Add a friendly time format for UI
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
    conn.commit()
    conn.close()
