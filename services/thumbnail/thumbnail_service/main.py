"""Thumbnail Generation Service: queue consumer + read API for thumbnail references."""

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
from vp_common.topology import Q_THUMBNAIL

from .config import Settings
from .models import Base, VideoThumbnail
from .worker import ThumbnailWorker, aggregate_status

log = logging.getLogger("thumbnail")


class ThumbnailOut(BaseModel):
    id: str
    video_id: str
    thumbnail_index: int
    timestamp_seconds: float | None
    relative_path: str | None
    width: int | None
    height: int | None
    size_bytes: int | None
    status: str
    attempts: int
    error_message: str | None
    updated_at: datetime


class ThumbnailList(BaseModel):
    video_id: str
    status: str
    items: list[ThumbnailOut]


def create_app(
    settings: Settings | None = None,
    *,
    broker: Broker | None = None,
    redis_client: Any = None,
    probe_fn=None,
    extract_fn=None,
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
    if extract_fn:
        extra["extract_fn"] = extract_fn
    worker = ThumbnailWorker(settings, db, broker, storage, LockManager(redis_client), **extra)

    def start_consumers() -> None:
        broker.consume(Q_THUMBNAIL, worker.handle, prefetch=2)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if settings.enable_background:
            start_consumers()
        yield
        broker.close()
        db.dispose()

    app = FastAPI(title="Thumbnail Generation Service", version="1.0.0", lifespan=lifespan)
    app.state.settings, app.state.db, app.state.broker = settings, db, broker
    app.state.worker, app.state.start_consumers = worker, start_consumers
    install_error_handlers(app)
    app.include_router(health_router(settings.service_name, db))

    @app.get("/internal/videos/{video_id}/thumbnails", response_model=ThumbnailList, tags=["internal"])
    def list_thumbnails(video_id: str = Path(pattern=UUID_PATTERN)):
        with db.session() as s:
            rows = s.scalars(
                select(VideoThumbnail).where(VideoThumbnail.video_id == video_id).order_by(VideoThumbnail.thumbnail_index)
            ).all()
            return ThumbnailList(
                video_id=video_id,
                status=aggregate_status([r.status for r in rows]),
                items=[ThumbnailOut(**{f: getattr(r, f) for f in ThumbnailOut.model_fields}) for r in rows],
            )

    return app
