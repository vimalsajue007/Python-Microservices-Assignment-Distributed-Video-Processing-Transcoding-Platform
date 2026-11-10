"""Video Upload Service.

Responsibilities: accept uploads, validate type and size, generate the video
id, persist the file + record, and publish ``video.uploaded``.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
from contextlib import asynccontextmanager
from datetime import timedelta

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

from vp_common.db import Database, new_id, utcnow
from vp_common.errors import APIError, error_body, install_error_handlers
from vp_common.events import VideoUploaded
from vp_common.logging import configure_logging
from vp_common.messaging import Broker, BrokerError, RabbitBroker
from vp_common.runtime import PeriodicTask, health_router
from vp_common.storage import Storage
from vp_common.topology import EX_EVENTS, RK_VIDEO_UPLOADED

from .config import Settings
from .models import Base, Video
from .validation import (
    SNIFF_BYTES,
    UploadValidationError,
    clean_title,
    validate_content_type,
    validate_filename,
    validate_signature,
)

log = logging.getLogger("upload")
CHUNK = 1024 * 1024


class VideoOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    title: str
    original_filename: str
    content_type: str
    size_bytes: int
    checksum_sha256: str
    status: str
    created_at: str

    @classmethod
    def from_row(cls, v: Video) -> "VideoOut":
        return cls(
            id=v.id, title=v.title, original_filename=v.original_filename,
            content_type=v.content_type, size_bytes=v.size_bytes,
            checksum_sha256=v.checksum_sha256, status=v.status,
            created_at=v.created_at.isoformat(),
        )


def _event_for(video: Video) -> VideoUploaded:
    return VideoUploaded(
        video_id=video.id,
        title=video.title,
        original_filename=video.original_filename,
        format=video.extension.lstrip("."),
        content_type=video.content_type,
        size_bytes=video.size_bytes,
        storage_path=video.storage_path,
        checksum_sha256=video.checksum_sha256,
        uploaded_at=video.created_at,
    )


class UploadService:
    def __init__(self, settings: Settings, db: Database, broker: Broker, storage: Storage):
        self.settings = settings
        self.db = db
        self.broker = broker
        self.storage = storage

    # -- core use-case --------------------------------------------------------
    def store_upload(self, upload: UploadFile, title: str | None) -> Video:
        name, ext = validate_filename(upload.filename)
        content_type = validate_content_type(ext, upload.content_type)
        title_clean = clean_title(title, name)

        src = upload.file
        header = src.read(SNIFF_BYTES)
        src.seek(0)
        if not header:
            raise UploadValidationError(400, "empty_file", "The uploaded file is empty")
        validate_signature(ext, header)

        video_id = new_id()
        relative = Storage.rel_upload(video_id, ext)
        final_path = self.storage.prepare(relative)
        part_path = final_path.with_suffix(final_path.suffix + ".part")

        digest = hashlib.sha256()
        total = 0
        try:
            with open(part_path, "wb") as out:
                while chunk := src.read(CHUNK):
                    total += len(chunk)
                    if total > self.settings.max_upload_bytes:
                        raise UploadValidationError(
                            413, "payload_too_large",
                            f"File exceeds the maximum size of {self.settings.max_upload_bytes // (1024 * 1024)} MiB",
                        )
                    digest.update(chunk)
                    out.write(chunk)
            part_path.replace(final_path)
        except BaseException:
            part_path.unlink(missing_ok=True)
            final_path.unlink(missing_ok=True)
            shutil.rmtree(final_path.parent, ignore_errors=True)
            raise

        video = Video(
            id=video_id, title=title_clean, original_filename=name, content_type=content_type,
            extension=ext, size_bytes=total, checksum_sha256=digest.hexdigest(),
            storage_path=relative, status="uploaded", event_published=False,
        )
        try:
            with self.db.session() as s:
                s.add(video)
        except BaseException:
            shutil.rmtree(final_path.parent, ignore_errors=True)
            raise

        self._publish(video)
        return video

    def _publish(self, video: Video) -> bool:
        try:
            self.broker.publish(
                EX_EVENTS, RK_VIDEO_UPLOADED, _event_for(video).to_body(), message_id=video.id
            )
        except BrokerError as exc:
            log.warning("video.uploaded for %s not published yet (%s); will retry", video.id, exc)
            return False
        with self.db.session() as s:
            row = s.get(Video, video.id)
            if row is not None:
                row.event_published = True
        return True

    def republish_pending(self) -> int:
        """Re-send events that could not be published (broker outage)."""
        cutoff = utcnow() - timedelta(seconds=self.settings.republish_grace_seconds)
        with self.db.session() as s:
            pending = list(
                s.scalars(
                    select(Video).where(Video.event_published.is_(False), Video.created_at < cutoff).limit(50)
                )
            )
        sent = 0
        for video in pending:
            if self._publish(video):
                sent += 1
        return sent


def create_app(settings: Settings | None = None, *, broker: Broker | None = None) -> FastAPI:
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
    service = UploadService(settings, db, broker, storage)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        tasks: list[PeriodicTask] = []
        if settings.enable_background:
            tasks.append(
                PeriodicTask("republisher", settings.republish_interval_seconds, service.republish_pending).start()
            )
        yield
        for t in tasks:
            t.stop()
        broker.close()
        db.dispose()

    app = FastAPI(title="Video Upload Service", version="1.0.0", lifespan=lifespan)
    app.state.settings, app.state.db, app.state.broker, app.state.service = settings, db, broker, service
    app.include_router(health_router(settings.service_name, db))

    @app.middleware("http")
    async def reject_oversized_early(request: Request, call_next):
        """Reject before the multipart body is parsed/spooled to disk."""
        if request.method == "POST" and request.url.path == "/videos/upload":
            raw = request.headers.get("content-length")
            rid = getattr(request.state, "request_id", None)
            if raw is None:
                return JSONResponse(
                    status_code=411,
                    content=error_body("length_required", "Content-Length header is required", None, rid),
                )
            try:
                length = int(raw)
            except ValueError:
                return JSONResponse(
                    status_code=400, content=error_body("bad_request", "Invalid Content-Length", None, rid)
                )
            limit = settings.max_upload_bytes + settings.multipart_overhead_bytes
            if length > limit:
                return JSONResponse(
                    status_code=413,
                    content=error_body(
                        "payload_too_large",
                        f"File exceeds the maximum size of {settings.max_upload_bytes // (1024 * 1024)} MiB",
                        None, rid,
                    ),
                )
        return await call_next(request)

    # installed last => outermost middleware, so request ids exist for every response
    install_error_handlers(app)

    @app.post("/videos/upload", status_code=201, response_model=VideoOut, tags=["videos"])
    async def upload_video(
        file: UploadFile = File(..., description="Video file (mp4, m4v, mov, mkv, webm, avi)"),
        title: str | None = Form(default=None, description="Optional display title"),
    ):
        try:
            video = await run_in_threadpool(service.store_upload, file, title)
        except UploadValidationError as exc:
            raise APIError(exc.status_code, exc.code, exc.message) from exc
        finally:
            await file.close()
        return VideoOut.from_row(video)

    return app
