"""Video Transcoding Worker Service: queue consumer + small read API for outputs/attempts."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import redis
from fastapi import FastAPI, Path
from pydantic import BaseModel
from sqlalchemy import select

from vp_common.db import Database
from vp_common.errors import UUID_PATTERN, install_error_handlers
from vp_common.locks import LockManager
from vp_common.logging import configure_logging
from vp_common.messaging import Broker, RabbitBroker
from vp_common.runtime import health_router
from vp_common.storage import Storage
from vp_common.topology import Q_TRANSCODE

from .config import Settings
from .models import Base, ProcessingAttempt, TranscodingOutput
from .worker import TranscodeWorker

log = logging.getLogger("transcoder")


class OutputOut(BaseModel):
    id: str
    video_id: str
    job_id: str
    resolution: str
    height: int
    width: int
    size_bytes: int
    video_codec: str
    relative_path: str
    status: str
    created_at: datetime


class OutputList(BaseModel):
    video_id: str
    outputs: list[OutputOut]


class AttemptOut(BaseModel):
    id: str
    job_id: str
    video_id: str
    attempt_number: int
    status: str
    worker_id: str | None
    error_type: str | None
    error_message: str | None
    duration_ms: int | None
    started_at: datetime
    finished_at: datetime | None


def create_app(
    settings: Settings | None = None,
    *,
    broker: Broker | None = None,
    redis_client: Any = None,
    probe_fn=None,
    transcode_fn=None,
) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.log_level, settings.service_name)

    db = Database(settings.database_url or "")
    if settings.auto_create_schema:
        Base.metadata.create_all(db.engine)
    storage = Storage(settings.storage_root)
    storage.ensure_root()
    broker = broker or RabbitBroker(
        settings.rabbitmq_url,
        max_redeliveries=settings.consumer_max_redeliveries,
        retry_delay_ms=settings.consumer_retry_delay_ms,
    )
    if redis_client is None:
        redis_client = redis.Redis.from_url(settings.redis_url, socket_connect_timeout=2, socket_timeout=2)

    extra = {}
    if probe_fn:
        extra["probe_fn"] = probe_fn
    if transcode_fn:
        extra["transcode_fn"] = transcode_fn
    worker = TranscodeWorker(settings, db, broker, storage, LockManager(redis_client), **extra)

    def start_consumers() -> None:
        broker.consume(Q_TRANSCODE, worker.handle, prefetch=settings.worker_concurrency)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if settings.enable_background:
            start_consumers()
        yield
        broker.close()
        db.dispose()

    app = FastAPI(title="Video Transcoding Worker Service", version="1.0.0", lifespan=lifespan)
    app.state.settings, app.state.db, app.state.broker = settings, db, broker
    app.state.worker, app.state.start_consumers = worker, start_consumers
    install_error_handlers(app)
    app.include_router(health_router(settings.service_name, db))

    @app.get("/internal/videos/{video_id}/outputs", response_model=OutputList, tags=["internal"])
    def list_outputs(video_id: str = Path(pattern=UUID_PATTERN)):
        with db.session() as s:
            rows = s.scalars(
                select(TranscodingOutput).where(TranscodingOutput.video_id == video_id).order_by(TranscodingOutput.height)
            ).all()
            return OutputList(
                video_id=video_id,
                outputs=[OutputOut(**{f: getattr(r, f) for f in OutputOut.model_fields}) for r in rows],
            )

    @app.get("/internal/jobs/{job_id}/attempts", response_model=list[AttemptOut], tags=["internal"])
    def list_attempts(job_id: str = Path(pattern=UUID_PATTERN)):
        with db.session() as s:
            rows = s.scalars(
                select(ProcessingAttempt).where(ProcessingAttempt.job_id == job_id).order_by(ProcessingAttempt.attempt_number)
            ).all()
            return [AttemptOut(**{f: getattr(r, f) for f in AttemptOut.model_fields}) for r in rows]

    return app
