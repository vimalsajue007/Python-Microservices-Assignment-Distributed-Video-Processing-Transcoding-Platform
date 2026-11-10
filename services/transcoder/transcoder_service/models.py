"""Tables owned by the Transcoding Worker (database ``vp_transcoder``)."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from vp_common.db import UTCDateTime, new_id, utcnow

ATTEMPT_RUNNING = "running"
ATTEMPT_SUCCEEDED = "succeeded"
ATTEMPT_FAILED = "failed"


class Base(DeclarativeBase):
    pass


class ProcessingAttempt(Base):
    """One row per (job, attempt number). The unique constraint is the
    authoritative guard against processing the same message twice."""

    __tablename__ = "processing_attempts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    job_id: Mapped[str] = mapped_column(String(36))
    video_id: Mapped[str] = mapped_column(String(36))
    attempt_number: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16), default=ATTEMPT_RUNNING)
    worker_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    message_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow)

    outputs: Mapped[list["TranscodingOutput"]] = relationship(back_populates="attempt")

    __table_args__ = (
        UniqueConstraint("job_id", "attempt_number", name="uq_attempt_job_number"),
        Index("ix_processing_attempts_video_id", "video_id"),
    )


class TranscodingOutput(Base):
    """A produced rendition. Only the latest rendition per (video, height) is kept."""

    __tablename__ = "transcoding_outputs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    video_id: Mapped[str] = mapped_column(String(36))
    job_id: Mapped[str] = mapped_column(String(36))
    attempt_id: Mapped[str | None] = mapped_column(ForeignKey("processing_attempts.id", ondelete="SET NULL"), nullable=True)
    resolution: Mapped[str] = mapped_column(String(16))
    height: Mapped[int] = mapped_column(Integer)
    width: Mapped[int] = mapped_column(Integer)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    video_codec: Mapped[str] = mapped_column(String(32), default="h264")
    relative_path: Mapped[str] = mapped_column(String(512))
    status: Mapped[str] = mapped_column(String(16), default="completed")
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)

    attempt: Mapped[ProcessingAttempt | None] = relationship(back_populates="outputs")

    __table_args__ = (
        UniqueConstraint("video_id", "height", name="uq_output_video_height"),
        Index("ix_transcoding_outputs_job_id", "job_id"),
    )
