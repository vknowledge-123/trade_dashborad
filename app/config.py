import os

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

KITE_ACCESS_TOKEN_KEY = "kite:access_token"
KITE_TOKEN_UPDATED_KEY = "kite:access_token_updated"

SESSION_SECRET_KEY = os.getenv("SESSION_SECRET_KEY", "change-this-secret")
SESSION_HTTPS_ONLY = os.getenv("SESSION_HTTPS_ONLY", "0") == "1"
SESSION_SAMESITE = os.getenv("SESSION_SAMESITE", "lax")

ADMIN_IP_ALLOWLIST = [ip.strip() for ip in os.getenv("ADMIN_IP_ALLOWLIST", "").split(",") if ip.strip()]
HCAPTCHA_SITE_KEY = os.getenv("HCAPTCHA_SITE_KEY")
HCAPTCHA_SECRET = os.getenv("HCAPTCHA_SECRET")

# Password hashing
# - scheme: "argon2id" (recommended) or "bcrypt"
# - pepper: extra server-side secret (keep out of DB/repo)
PASSWORD_HASH_SCHEME = os.getenv("PASSWORD_HASH_SCHEME", "bcrypt").lower().strip()
PASSWORD_PEPPER = os.getenv("PASSWORD_PEPPER", "")
BCRYPT_ROUNDS = int(os.getenv("BCRYPT_ROUNDS", "12"))
