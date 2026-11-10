"""HMAC-signed, expiring URLs for media delivery."""

from __future__ import annotations

import hashlib
import hmac
import time


def sign(secret: str, path: str, expires: int) -> str:
    return hmac.new(secret.encode(), f"{path}:{expires}".encode(), hashlib.sha256).hexdigest()


def verify(secret: str, path: str, expires: int, signature: str, *, now: float | None = None) -> bool:
    if expires < (now if now is not None else time.time()):
        return False
    return hmac.compare_digest(sign(secret, path, expires), signature or "")
