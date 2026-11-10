"""Test doubles: in-memory Redis, host-routing ASGI transport."""

from __future__ import annotations

import time

import httpx


class FakeRedis:
    """The tiny subset of the redis client API used by LockManager."""

    def __init__(self):
        self._data: dict[str, tuple[str, float | None]] = {}
        self.down = False  # simulate an outage

    def _check(self):
        if self.down:
            raise ConnectionError("redis is down")

    def _alive(self, key: str):
        item = self._data.get(key)
        if item is None:
            return None
        value, expires = item
        if expires is not None and expires < time.monotonic():
            del self._data[key]
            return None
        return value

    def set(self, key, value, nx=False, ex=None):
        self._check()
        if nx and self._alive(key) is not None:
            return None
        self._data[key] = (value, time.monotonic() + ex if ex else None)
        return True

    def get(self, key):
        self._check()
        return self._alive(key)

    def delete(self, key):
        self._check()
        self._data.pop(key, None)

    def expire(self, key, seconds):
        self._check()
        value = self._alive(key)
        if value is not None:
            self._data[key] = (value, time.monotonic() + seconds)

    def expire_now(self, key):
        """Test helper: make a lock expire immediately (simulates a dead worker)."""
        self._data.pop(key, None)

    def keys(self):
        return [k for k in list(self._data) if self._alive(k) is not None]


class HostRouterTransport(httpx.AsyncBaseTransport):
    """Routes requests to different ASGI apps by hostname (``http://metadata/...``)."""

    def __init__(self, apps: dict[str, object]):
        self._transports = {host: httpx.ASGITransport(app=app) for host, app in apps.items()}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        transport = self._transports.get(request.url.host)
        if transport is None:
            raise httpx.ConnectError(f"no service registered for host {request.url.host!r}", request=request)
        return await transport.handle_async_request(request)


class DownTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)
