"""Tables owned by the Metadata service (database ``vp_metadata``)."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, Float, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from vp_common.db import UTCDateTime, utcnow


class Base(DeclarativeBase):
    pass


# Video lifecycle (derived from upload + job status events)
STATUS_UPLOADED = "uploaded"
STATUS_QUEUED = "queued"
STATUS_PROCESSING = "processing"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_INVALID = "invalid"  # file passed upload checks but is not decodable media

ANALYSIS_PENDING = "pending"
ANALYSIS_OK = "ok"
ANALYSIS_FAILED = "failed"


class VideoMetadata(Base):
    __tablename__ = "video_metadata"

    video_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    title: Mapped[str] = mapped_column(String(255))
    original_filename: Mapped[str] = mapped_column(String(255))
    format: Mapped[str] = mapped_column(String(16))
    container: Mapped[str | None] = mapped_column(String(64), nullable=True)
    video_codec: Mapped[str | None] = mapped_column(String(32), nullable=True)
    audio_codec: Mapped[str | None] = mapped_column(String(32), nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    frame_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    bit_rate: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    storage_path: Mapped[str] = mapped_column(String(512))

    status: Mapped[str] = mapped_column(String(32), default=STATUS_UPLOADED)
    analysis_status: Mapped[str] = mapped_column(String(16), default=ANALYSIS_PENDING)
    progress: Mapped[int] = mapped_column(Integer, default=0)
    latest_job_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_event_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        Index("ix_video_metadata_status", "status"),
        Index("ix_video_metadata_created_at", "created_at"),
    )
