"""Small Redis-backed lock helper (SET NX PX with owner token)."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

log = logging.getLogger("vp.locks")


class LockUnavailable(Exception):
    """Redis could not be reached; callers decide whether to fail open."""


class LockBusy(Exception):
    """The lock is held by someone else."""


class LockManager:
    def __init__(self, client, prefix: str = "vp:lock:"):
        self._client = client
        self._prefix = prefix

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def acquire(self, key: str, owner: str, ttl_seconds: int) -> bool:
        """True if acquired (or already held by ``owner``), False if held by someone else."""
        try:
            if self._client.set(self._key(key), owner, nx=True, ex=ttl_seconds):
                return True
            current = self._client.get(self._key(key))
            if isinstance(current, bytes):
                current = current.decode()
            return current == owner
        except Exception as exc:  # noqa: BLE001
            raise LockUnavailable(str(exc)) from exc

    def extend(self, key: str, owner: str, ttl_seconds: int) -> None:
        try:
            current = self._client.get(self._key(key))
            if isinstance(current, bytes):
                current = current.decode()
            if current == owner:
                self._client.expire(self._key(key), ttl_seconds)
        except Exception as exc:  # noqa: BLE001
            log.warning("could not extend lock %s: %s", key, exc)

    def release(self, key: str, owner: str) -> None:
        try:
            current = self._client.get(self._key(key))
            if isinstance(current, bytes):
                current = current.decode()
            if current == owner:
                self._client.delete(self._key(key))
        except Exception as exc:  # noqa: BLE001
            log.warning("could not release lock %s: %s", key, exc)

    @contextmanager
    def hold(self, key: str, owner: str, ttl_seconds: int, *, fail_open: bool = True) -> Iterator[None]:
        """Context manager. Raises :class:`LockBusy` if held elsewhere."""
        try:
            acquired = self.acquire(key, owner, ttl_seconds)
        except LockUnavailable:
            if not fail_open:
                raise
            log.warning("redis unavailable, proceeding without lock %s", key)
            yield
            return
        if not acquired:
            raise LockBusy(key)
        try:
            yield
        finally:
            self.release(key, owner)
