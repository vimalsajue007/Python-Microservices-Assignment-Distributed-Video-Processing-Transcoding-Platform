"""Tables owned by the Thumbnail service (database ``vp_thumbnail``)."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, Float, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from vp_common.db import UTCDateTime, new_id, utcnow

THUMB_PENDING = "pending"
THUMB_COMPLETED = "completed"
THUMB_FAILED = "failed"


class Base(DeclarativeBase):
    pass


class VideoThumbnail(Base):
    __tablename__ = "video_thumbnails"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    video_id: Mapped[str] = mapped_column(String(36))
    thumbnail_index: Mapped[int] = mapped_column(Integer)
    timestamp_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    relative_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    status: Mapped[str] = mapped_column(String(16), default=THUMB_PENDING)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        UniqueConstraint("video_id", "thumbnail_index", name="uq_thumbnail_video_index"),
        Index("ix_video_thumbnails_video_id", "video_id"),
    )
