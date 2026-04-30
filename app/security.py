from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional

import bcrypt

from app.config import BCRYPT_ROUNDS, PASSWORD_HASH_SCHEME, PASSWORD_PEPPER

try:
    from argon2 import PasswordHasher
    from argon2.exceptions import VerifyMismatchError
except Exception:  # pragma: no cover
    PasswordHasher = None  # type: ignore[assignment]
    VerifyMismatchError = Exception  # type: ignore[assignment]


@dataclass(frozen=True)
class PasswordVerifyResult:
    ok: bool
    used_pepper: bool
    scheme: str  # "argon2id" | "bcrypt" | "legacy-sha256" | "unknown"


_argon2_hasher: Optional["PasswordHasher"] = None


def _get_argon2_hasher() -> "PasswordHasher":
    global _argon2_hasher
    if PasswordHasher is None:
        raise RuntimeError(
            "PASSWORD_HASH_SCHEME=argon2id requires dependency 'argon2-cffi'. "
            "Install it and restart."
        )
    if _argon2_hasher is None:
        _argon2_hasher = PasswordHasher()
    return _argon2_hasher


def _pepper_password(password: str) -> str:
    if not PASSWORD_PEPPER:
        return password
    return f"{password}{PASSWORD_PEPPER}"


def hash_password(password: str) -> str:
    """
    Return a one-way hash suitable for storage.
    Supports bcrypt and argon2id, selected by PASSWORD_HASH_SCHEME.
    """
    if PASSWORD_HASH_SCHEME == "argon2id":
        hasher = _get_argon2_hasher()
        return hasher.hash(_pepper_password(password))

    if PASSWORD_HASH_SCHEME != "bcrypt":
        raise ValueError("PASSWORD_HASH_SCHEME must be 'bcrypt' or 'argon2id'")

    salt = bcrypt.gensalt(rounds=BCRYPT_ROUNDS)
    return bcrypt.hashpw(_pepper_password(password).encode("utf-8"), salt).decode("utf-8")


def verify_password(password: str, stored_hash: str) -> PasswordVerifyResult:
    """
    Verify password against stored hash.
    To support migration, if PASSWORD_PEPPER is set, this tries with pepper first,
    then falls back to without pepper.
    """
    if not stored_hash:
        return PasswordVerifyResult(ok=False, used_pepper=False, scheme="unknown")

    # Argon2 PHC strings look like: $argon2id$v=19$...
    if stored_hash.startswith("$argon2"):
        try:
            hasher = _get_argon2_hasher()
        except Exception:
            return PasswordVerifyResult(ok=False, used_pepper=False, scheme="argon2id")

        if PASSWORD_PEPPER:
            try:
                if hasher.verify(stored_hash, _pepper_password(password)):
                    return PasswordVerifyResult(ok=True, used_pepper=True, scheme="argon2id")
            except VerifyMismatchError:
                pass
        try:
            if hasher.verify(stored_hash, password):
                return PasswordVerifyResult(ok=True, used_pepper=False, scheme="argon2id")
        except VerifyMismatchError:
            return PasswordVerifyResult(ok=False, used_pepper=False, scheme="argon2id")

        return PasswordVerifyResult(ok=False, used_pepper=False, scheme="argon2id")

    # bcrypt hashes begin with $2a$, $2b$, $2y$
    if stored_hash.startswith("$2"):
        stored_bytes = stored_hash.encode("utf-8")
        if PASSWORD_PEPPER:
            try:
                if bcrypt.checkpw(_pepper_password(password).encode("utf-8"), stored_bytes):
                    return PasswordVerifyResult(ok=True, used_pepper=True, scheme="bcrypt")
            except Exception:
                pass
        try:
            if bcrypt.checkpw(password.encode("utf-8"), stored_bytes):
                return PasswordVerifyResult(ok=True, used_pepper=False, scheme="bcrypt")
        except Exception:
            return PasswordVerifyResult(ok=False, used_pepper=False, scheme="bcrypt")
        return PasswordVerifyResult(ok=False, used_pepper=False, scheme="bcrypt")

    # Legacy SHA256 (not salted) migration support
    legacy_plain = hashlib.sha256(password.encode("utf-8")).hexdigest()
    if legacy_plain == stored_hash:
        return PasswordVerifyResult(ok=True, used_pepper=False, scheme="legacy-sha256")
    if PASSWORD_PEPPER:
        legacy_peppered = hashlib.sha256(_pepper_password(password).encode("utf-8")).hexdigest()
        if legacy_peppered == stored_hash:
            return PasswordVerifyResult(ok=True, used_pepper=True, scheme="legacy-sha256")

    return PasswordVerifyResult(ok=False, used_pepper=False, scheme="unknown")


def _bcrypt_cost(stored_hash: str) -> Optional[int]:
    try:
        # $2b$12$...
        parts = stored_hash.split("$")
        if len(parts) >= 3:
            return int(parts[2])
    except Exception:
        return None
    return None


def password_needs_rehash(stored_hash: str) -> bool:
    if not stored_hash:
        return True

    if PASSWORD_HASH_SCHEME == "argon2id":
        if not stored_hash.startswith("$argon2"):
            return True
        try:
            hasher = _get_argon2_hasher()
            return hasher.check_needs_rehash(stored_hash)
        except Exception:
            return False

    if PASSWORD_HASH_SCHEME == "bcrypt":
        if not stored_hash.startswith("$2"):
            return True
        cost = _bcrypt_cost(stored_hash)
        return cost is not None and cost < BCRYPT_ROUNDS

    return True


def should_upgrade_password_hash(
    stored_hash: str, verify_result: PasswordVerifyResult
) -> bool:
    if not verify_result.ok:
        return False
    if verify_result.scheme == "legacy-sha256":
        return True
    if not verify_result.used_pepper and bool(PASSWORD_PEPPER):
        return True
    return password_needs_rehash(stored_hash)
