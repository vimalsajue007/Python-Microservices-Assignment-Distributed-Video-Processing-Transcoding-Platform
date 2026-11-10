"""Video Metadata Service.

Owns descriptive metadata (title, format, duration, dimensions, size) and the
*processing status* of each video. It is a pure event consumer on the write
side (``video.uploaded``, ``job.status_changed``) and a read API for everyone else.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Path, Query, Request
from pydantic import BaseModel, ValidationError
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError

from vp_common.db import Database, utcnow
from vp_common.errors import UUID_PATTERN, APIError, install_error_handlers
from vp_common.events import (
    JOB_COMPLETED, JOB_FAILED, JOB_QUEUED, JOB_RETRYING, JOB_RUNNING,
    JobStatusChanged, VideoUploaded,
)
from vp_common.ffmpeg import FFmpegError, probe
from vp_common.logging import configure_logging
from vp_common.messaging import Broker, Message, RabbitBroker, RejectMessage
from vp_common.runtime import health_router
from vp_common.storage import Storage
from vp_common.topology import Q_METADATA, RK_JOB_STATUS, RK_VIDEO_UPLOADED

from .config import Settings
from .models import (
    ANALYSIS_FAILED, ANALYSIS_OK, STATUS_COMPLETED, STATUS_FAILED, STATUS_INVALID,
    STATUS_PROCESSING, STATUS_QUEUED, STATUS_UPLOADED, Base, VideoMetadata,
)

log = logging.getLogger("metadata")

JOB_TO_VIDEO_STATUS = {
    JOB_QUEUED: STATUS_QUEUED,
    JOB_RUNNING: STATUS_PROCESSING,
    JOB_RETRYING: STATUS_PROCESSING,
    JOB_COMPLETED: STATUS_COMPLETED,
    JOB_FAILED: STATUS_FAILED,
}


# --------------------------------------------------------------------------- #
# Schemas
# --------------------------------------------------------------------------- #
class VideoOut(BaseModel):
    video_id: str
    title: str
    original_filename: str
    format: str
    container: str | None
    video_codec: str | None
    audio_codec: str | None
    duration_seconds: float | None
    width: int | None
    height: int | None
    frame_rate: float | None
    bit_rate: int | None
    size_bytes: int
    status: str
    progress: int
    latest_job_id: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, r: VideoMetadata) -> "VideoOut":
        return cls(**{name: getattr(r, name) for name in cls.model_fields})


class InternalVideoOut(VideoOut):
    storage_path: str
    analysis_status: str

    @classmethod
    def from_row(cls, r: VideoMetadata) -> "InternalVideoOut":
        return cls(**{name: getattr(r, name) for name in cls.model_fields})


class VideoList(BaseModel):
    items: list[VideoOut]
    total: int
    limit: int
    offset: int


class StatusOut(BaseModel):
    video_id: str
    status: str
    progress: int
    job_id: str | None
    error_message: str | None
    updated_at: datetime


# --------------------------------------------------------------------------- #
# Event handling
# --------------------------------------------------------------------------- #
class MetadataService:
    def __init__(self, settings: Settings, db: Database, storage: Storage):
        self.settings = settings
        self.db = db
        self.storage = storage

    def handle(self, msg: Message) -> None:
        try:
            if msg.routing_key == RK_VIDEO_UPLOADED:
                self.on_video_uploaded(VideoUploaded.model_validate(msg.body))
            elif msg.routing_key == RK_JOB_STATUS:
                self.on_job_status(JobStatusChanged.model_validate(msg.body))
            else:
                log.warning("ignoring unexpected routing key %s", msg.routing_key)
        except ValidationError as exc:
            raise RejectMessage(f"invalid {msg.routing_key} payload: {exc.errors()[:2]}") from exc

    def on_video_uploaded(self, ev: VideoUploaded) -> None:
        record = VideoMetadata(
            video_id=ev.video_id, title=ev.title, original_filename=ev.original_filename,
            format=ev.format, size_bytes=ev.size_bytes, storage_path=ev.storage_path,
            status=STATUS_UPLOADED, created_at=ev.uploaded_at,
        )
        try:
            with self.db.session() as s:
                s.add(record)
        except IntegrityError:
            log.info("duplicate video.uploaded for %s ignored", ev.video_id)
            return

        # Analyse outside the transaction: ffprobe can take a moment.
        try:
            info = probe(self.storage.resolve(ev.storage_path), ffprobe_bin=self.settings.ffprobe_bin)
        except FFmpegError as exc:
            if exc.retryable:
                raise  # transient (timeout): let the consumer redeliver
            with self.db.session() as s:
                row = s.get(VideoMetadata, ev.video_id)
                row.analysis_status = ANALYSIS_FAILED
                row.status = STATUS_INVALID
                row.error_message = str(exc)[:1000]
            log.warning("video %s is not decodable: %s", ev.video_id, exc)
            return

        with self.db.session() as s:
            row = s.get(VideoMetadata, ev.video_id)
            row.analysis_status = ANALYSIS_OK
            row.container = info.container
            row.video_codec = info.video_codec
            row.audio_codec = info.audio_codec
            row.duration_seconds = info.duration
            row.width, row.height = info.width, info.height
            row.frame_rate = info.frame_rate
            row.bit_rate = info.bit_rate

    def on_job_status(self, ev: JobStatusChanged) -> None:
        mapped = JOB_TO_VIDEO_STATUS.get(ev.status)
        if mapped is None:
            raise RejectMessage(f"unknown job status {ev.status!r}")
        with self.db.session() as s:
            row = s.get(VideoMetadata, ev.video_id, with_for_update=True)
            if row is None:
                log.warning("job status for unknown video %s ignored", ev.video_id)
                return
            if row.status_event_at is not None and ev.occurred_at < row.status_event_at:
                return  # stale / out-of-order event
            if row.status == STATUS_INVALID:
                return
            row.status = mapped
            row.progress = 100 if mapped == STATUS_COMPLETED else max(0, min(ev.progress, 100))
            row.latest_job_id = ev.job_id
            row.error_message = ev.error_message if mapped in (STATUS_FAILED,) or ev.error_message else None
            row.status_event_at = ev.occurred_at


# --------------------------------------------------------------------------- #
# App factory
# --------------------------------------------------------------------------- #
def create_app(settings: Settings | None = None, *, broker: Broker | None = None) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.log_level, settings.service_name)

    db = Database(settings.database_url or "")
    if settings.auto_create_schema:
        Base.metadata.create_all(db.engine)
    storage = Storage(settings.storage_root)
    service = MetadataService(settings, db, storage)
    broker = broker or RabbitBroker(
        settings.rabbitmq_url,
        max_redeliveries=settings.consumer_max_redeliveries,
        retry_delay_ms=settings.consumer_retry_delay_ms,
    )

    def start_consumers() -> None:
        broker.consume(Q_METADATA, service.handle, prefetch=4)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if settings.enable_background:
            start_consumers()
        yield
        broker.close()
        db.dispose()

    app = FastAPI(title="Video Metadata Service", version="1.0.0", lifespan=lifespan)
    app.state.settings, app.state.db, app.state.broker = settings, db, broker
    app.state.service, app.state.start_consumers = service, start_consumers
    install_error_handlers(app)
    app.include_router(health_router(settings.service_name, db))

    @app.get("/videos", response_model=VideoList, tags=["videos"])
    def list_videos(
        status: str | None = Query(default=None, description="Filter by status"),
        q: str | None = Query(default=None, max_length=100, description="Search title / filename"),
        limit: int = Query(default=50, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
    ):
        stmt = select(VideoMetadata)
        count = select(func.count()).select_from(VideoMetadata)
        if status:
            stmt, count = stmt.where(VideoMetadata.status == status), count.where(VideoMetadata.status == status)
        if q:
            like = f"%{q}%"
            cond = or_(VideoMetadata.title.like(like), VideoMetadata.original_filename.like(like))
            stmt, count = stmt.where(cond), count.where(cond)
        with db.session() as s:
            total = s.scalar(count) or 0
            rows = s.scalars(stmt.order_by(VideoMetadata.created_at.desc()).limit(limit).offset(offset)).all()
            return VideoList(items=[VideoOut.from_row(r) for r in rows], total=total, limit=limit, offset=offset)

    @app.get("/videos/{video_id}", response_model=VideoOut, tags=["videos"])
    def get_video(video_id: str = Path(pattern=UUID_PATTERN)):
        with db.session() as s:
            row = s.get(VideoMetadata, video_id)
            if row is None:
                raise APIError(404, "video_not_found", f"Video {video_id} not found")
            return VideoOut.from_row(row)

    @app.get("/videos/{video_id}/status", response_model=StatusOut, tags=["videos"])
    def get_status(video_id: str = Path(pattern=UUID_PATTERN)):
        with db.session() as s:
            row = s.get(VideoMetadata, video_id)
            if row is None:
                raise APIError(404, "video_not_found", f"Video {video_id} not found")
            return StatusOut(
                video_id=row.video_id, status=row.status, progress=row.progress,
                job_id=row.latest_job_id, error_message=row.error_message, updated_at=row.updated_at,
            )

    @app.get("/internal/videos/{video_id}", response_model=InternalVideoOut, tags=["internal"])
    def get_video_internal(video_id: str = Path(pattern=UUID_PATTERN)):
        """Service-to-service lookup (includes storage path). Not exposed by the gateway."""
        with db.session() as s:
            row = s.get(VideoMetadata, video_id)
            if row is None:
                raise APIError(404, "video_not_found", f"Video {video_id} not found")
            return InternalVideoOut.from_row(row)

    return app
