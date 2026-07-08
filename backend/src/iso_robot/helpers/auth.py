from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt

from iso_robot.config import get_settings


# ─────────────────────────────────────────────────────────────────────────────
# Password hashing — UNCHANGED (SHA-256 + fixed salt). Fine for the demo.
# Ayush: "for demo no encryption works as well." Existing users keep working.
# ─────────────────────────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    salt = "iso-robot-salt-v1:"
    return hashlib.sha256(f"{salt}{password}".encode("utf-8")).hexdigest()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return hash_password(plain_password) == hashed_password


# ─────────────────────────────────────────────────────────────────────────────
# Tokens — real JWT (HS256). Sliding window: the token lifetime is jwt_idle_minutes,
# and every authenticated request re-issues a fresh token (see deps.get_current_user).
# Same function signatures as before, so handlers/auth.py does NOT change.
# ─────────────────────────────────────────────────────────────────────────────

def create_token(
    user_id: str,
    client_org_id: str,
    role: str,
    email: str = "",
    name: str = "",
) -> str:
    settings = get_settings()
    now = datetime.now(tz=timezone.utc)
    payload = {
        "sub": user_id,
        "org": client_org_id,
        "role": role,
        "email": email,
        "name": name,
        "iat": now,
        "exp": now + timedelta(minutes=settings.jwt_idle_minutes),
    }
    token = jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)
    return token if isinstance(token, str) else token.decode("utf-8")


def decode_token(token: str) -> Optional[dict]:
    settings = get_settings()
    try:
        return jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
    except jwt.PyJWTError:
        return None