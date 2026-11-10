"""Tables owned by the Orchestrator service (database ``vp_orchestrator``)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from vp_common.db import UTCDateTime, new_id, utcnow


class Base(DeclarativeBase):
    pass


class ProcessingJob(Base):
    __tablename__ = "processing_jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    video_id: Mapped[str] = mapped_column(String(36))
    job_type: Mapped[str] = mapped_column(String(32), default="transcode")
    status: Mapped[str] = mapped_column(String(16), default="queued")
    progress: Mapped[int] = mapped_column(Integer, default=0)

    requested_resolutions: Mapped[list[int]] = mapped_column(JSON)
    source_path: Mapped[str] = mapped_column(String(512))

    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    current_attempt: Mapped[int] = mapped_column(Integer, default=1)
    attempt_history: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    next_retry_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    last_published_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    dead_lettered_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        Index("ix_processing_jobs_video_id", "video_id"),
        Index("ix_processing_jobs_status", "status"),
        Index("ix_processing_jobs_next_retry_at", "next_retry_at"),
    )
