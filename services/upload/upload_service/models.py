"""Tables owned by the Upload service (database ``vp_upload``)."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, Boolean, Index, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from vp_common.db import UTCDateTime, new_id, utcnow


class Base(DeclarativeBase):
    pass


class Video(Base):
    __tablename__ = "videos"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    title: Mapped[str] = mapped_column(String(255))
    original_filename: Mapped[str] = mapped_column(String(255))
    content_type: Mapped[str] = mapped_column(String(100))
    extension: Mapped[str] = mapped_column(String(16))
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    checksum_sha256: Mapped[str] = mapped_column(String(64))
    storage_path: Mapped[str] = mapped_column(String(512))
    status: Mapped[str] = mapped_column(String(32), default="uploaded")
    # False until the video.uploaded event reached the broker (mini transactional outbox)
    event_published: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        Index("ix_videos_created_at", "created_at"),
        Index("ix_videos_event_published", "event_published"),
    )
