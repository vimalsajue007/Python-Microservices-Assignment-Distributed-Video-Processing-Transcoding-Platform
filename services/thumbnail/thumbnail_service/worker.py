"""Thumbnail generation: consumes ``video.uploaded`` and extracts N evenly spaced frames."""

from __future__ import annotations

import logging
import os

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from vp_common import ffmpeg as ff
from vp_common.db import Database, new_id, utcnow
from vp_common.events import ThumbnailGenerated, VideoUploaded
from vp_common.locks import LockManager, LockUnavailable
from vp_common.messaging import Broker, Message, RejectMessage
from vp_common.storage import Storage
from vp_common.topology import EX_EVENTS, RK_THUMBNAIL, RK_VIDEO_UPLOADED

from .config import Settings
from .models import THUMB_COMPLETED, THUMB_FAILED, THUMB_PENDING, VideoThumbnail

log = logging.getLogger("thumbnail")


def aggregate_status(statuses: list[str]) -> str:
    if not statuses:
        return "none"
    if all(s == THUMB_COMPLETED for s in statuses):
        return "completed"
    if any(s == THUMB_PENDING for s in statuses):
        return "processing"
    if all(s == THUMB_FAILED for s in statuses):
        return "failed"
    return "partial"


class ThumbnailWorker:
    def __init__(
        self, settings: Settings, db: Database, broker: Broker, storage: Storage, locks: LockManager,
        *, probe_fn=ff.probe, extract_fn=ff.extract_frame,
    ):
        self.settings = settings
        self.db = db
        self.broker = broker
        self.storage = storage
        self.locks = locks
        self.probe = probe_fn
        self.extract = extract_fn

    def handle(self, msg: Message) -> None:
        if msg.routing_key != RK_VIDEO_UPLOADED:
            return
        try:
            event = VideoUploaded.model_validate(msg.body)
        except ValidationError as exc:
            raise RejectMessage(f"invalid video.uploaded payload: {exc.errors()[:2]}") from exc

        key, owner = f"thumbnails:{event.video_id}", new_id()
        try:
            if not self.locks.acquire(key, owner, self.settings.lock_ttl_seconds):
                log.info("thumbnails for %s are already being generated", event.video_id)
                return
        except LockUnavailable:
            pass  # the unique (video_id, index) rows still prevent duplicates
        try:
            self.generate(event)
        finally:
            self.locks.release(key, owner)

    # ------------------------------------------------------------------ #
    def _ensure_rows(self, video_id: str) -> None:
        with self.db.session() as s:
            existing = set(s.scalars(select(VideoThumbnail.thumbnail_index).where(VideoThumbnail.video_id == video_id)))
            for index in range(self.settings.thumbnail_count):
                if index not in existing:
                    s.add(VideoThumbnail(video_id=video_id, thumbnail_index=index, status=THUMB_PENDING))

    def generate(self, event: VideoUploaded) -> None:
        try:
            self._ensure_rows(event.video_id)
        except IntegrityError:
            pass  # concurrent creation: rows exist, carry on

        with self.db.session() as s:
            todo = [
                (r.id, r.thumbnail_index)
                for r in s.scalars(
                    select(VideoThumbnail)
                    .where(VideoThumbnail.video_id == event.video_id, VideoThumbnail.status != THUMB_COMPLETED)
                    .order_by(VideoThumbnail.thumbnail_index)
                )
            ]
        if not todo:
            log.info("thumbnails for %s already complete (duplicate event)", event.video_id)
            return

        source = self.storage.resolve(event.storage_path)
        try:
            info = self.probe(source, ffprobe_bin=self.settings.ffprobe_bin)
        except ff.FFmpegError as exc:
            if exc.retryable:
                raise  # transient: let the broker redeliver
            self._fail_all([t[0] for t in todo], f"Unreadable video: {exc}")
            return
        except FileNotFoundError as exc:
            raise exc  # shared volume not ready yet: redelivery

        count = self.settings.thumbnail_count
        for thumb_id, index in todo:
            at = info.duration * (index + 1) / (count + 1) if info.duration > 0 else 0.0
            self._make_one(event, source, thumb_id, index, at)

    def _fail_all(self, ids: list[str], message: str) -> None:
        with self.db.session() as s:
            for row in s.scalars(select(VideoThumbnail).where(VideoThumbnail.id.in_(ids))):
                row.status = THUMB_FAILED
                row.error_message = message[:1000]
                row.attempts += 1

    def _make_one(self, event: VideoUploaded, source, thumb_id: str, index: int, at: float) -> None:
        relative = Storage.rel_thumbnail(event.video_id, index)
        final = self.storage.prepare(relative)
        tmp = final.with_name(f".{final.name}.tmp")
        error = ""
        attempts_used = 0
        for attempt in range(1, self.settings.ffmpeg_attempts + 1):
            attempts_used = attempt
            try:
                self.extract(source, tmp, at, max_width=self.settings.thumbnail_max_width, ffmpeg_bin=self.settings.ffmpeg_bin)
                os.replace(tmp, final)
                break
            except (ff.FFmpegError, OSError) as exc:
                error = str(exc)
                tmp.unlink(missing_ok=True)
                at = 0.0  # fall back to the first frame on retry
                log.warning("thumbnail %s/%d attempt %d failed: %s", event.video_id, index, attempt, exc)
        else:
            with self.db.session() as s:
                row = s.get(VideoThumbnail, thumb_id)
                row.status, row.error_message, row.attempts = THUMB_FAILED, error[:1000], row.attempts + attempts_used
                row.timestamp_seconds = at
            return

        width = height = None
        try:
            probed = self.probe(final, ffprobe_bin=self.settings.ffprobe_bin)
            width, height = probed.width, probed.height
        except ff.FFmpegError:
            pass
        with self.db.session() as s:
            row = s.get(VideoThumbnail, thumb_id)
            row.status = THUMB_COMPLETED
            row.relative_path = relative
            row.timestamp_seconds = round(at, 3)
            row.width, row.height = width, height
            row.size_bytes = final.stat().st_size
            row.error_message = None
            row.attempts += attempts_used
        try:
            self.broker.publish(
                EX_EVENTS, RK_THUMBNAIL,
                ThumbnailGenerated(
                    video_id=event.video_id, thumbnail_id=thumb_id, index=index,
                    status=THUMB_COMPLETED, relative_path=relative,
                ).to_body(),
            )
        except Exception:  # noqa: BLE001 - informational event only
            log.warning("could not publish thumbnail.generated for %s", event.video_id)
