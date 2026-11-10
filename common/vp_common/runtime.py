"""Small helpers shared by service app factories."""

from __future__ import annotations

import logging
import threading
from typing import Callable

from fastapi import APIRouter, FastAPI

from .db import Database

log = logging.getLogger("vp.runtime")


def health_router(service: str, db: Database | None = None) -> APIRouter:
    router = APIRouter(tags=["health"])

    @router.get("/health")
    def health():
        return {"status": "ok", "service": service}

    @router.get("/ready")
    def ready():
        ok = db.ping() if db is not None else True
        return {"status": "ready" if ok else "degraded", "service": service, "database": ok}

    return router


class PeriodicTask:
    """Runs ``fn`` every ``interval`` seconds on a daemon thread until stopped."""

    def __init__(self, name: str, interval: float, fn: Callable[[], None]):
        self._fn = fn
        self._interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name=name, daemon=True)

    def start(self) -> "PeriodicTask":
        self._thread.start()
        return self

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._fn()
            except Exception:  # noqa: BLE001
                log.exception("periodic task %s failed", self._thread.name)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)


def attach_background(app: FastAPI, tasks: list[PeriodicTask]) -> None:
    app.state.background_tasks = tasks
