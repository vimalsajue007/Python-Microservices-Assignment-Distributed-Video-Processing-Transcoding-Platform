"""Message contracts exchanged between services (the async API of the platform).

Contracts are versioned implicitly by field additions only: consumers ignore
unknown fields and producers never remove or repurpose existing ones.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .db import new_id, utcnow

# Processing status vocabulary ------------------------------------------------
JOB_QUEUED = "queued"
JOB_RUNNING = "running"
JOB_RETRYING = "retrying"
JOB_COMPLETED = "completed"
JOB_FAILED = "failed"
JOB_ACTIVE_STATUSES = (JOB_QUEUED, JOB_RUNNING, JOB_RETRYING)
JOB_TERMINAL_STATUSES = (JOB_COMPLETED, JOB_FAILED)

ALLOWED_RESOLUTIONS = (144, 240, 360, 480, 720, 1080, 1440, 2160)
DEFAULT_RESOLUTIONS = (360, 720, 1080)


class Event(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str = Field(default_factory=new_id)
    occurred_at: datetime = Field(default_factory=utcnow)

    def to_body(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class VideoUploaded(Event):
    video_id: str
    title: str
    original_filename: str
    format: str
    content_type: str
    size_bytes: int
    storage_path: str  # relative to STORAGE_ROOT
    checksum_sha256: str
    uploaded_at: datetime


class TranscodeJob(BaseModel):
    """Work message published by the orchestrator on ``vp.jobs`` / ``transcode``."""

    model_config = ConfigDict(extra="ignore")

    message_id: str = Field(default_factory=new_id)
    job_id: str
    video_id: str
    source_path: str
    resolutions: list[int] = Field(min_length=1)
    attempt: int = Field(ge=1)
    max_attempts: int = Field(ge=1)
    dispatched_at: datetime = Field(default_factory=utcnow)

    def to_body(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class JobStatusChanged(Event):
    job_id: str
    video_id: str
    status: str
    progress: int = 0
    attempt: int = 0
    max_attempts: int = 0
    error_message: str | None = None


class AttemptStarted(Event):
    job_id: str
    video_id: str
    attempt: int
    worker_id: str


class JobProgress(Event):
    job_id: str
    video_id: str
    attempt: int
    progress: int
    current_resolution: int | None = None


class OutputInfo(BaseModel):
    resolution: str
    height: int
    width: int
    size_bytes: int
    relative_path: str


class AttemptSucceeded(Event):
    job_id: str
    video_id: str
    attempt: int
    worker_id: str
    duration_ms: int
    outputs: list[OutputInfo]


class AttemptFailed(Event):
    job_id: str
    video_id: str
    attempt: int
    worker_id: str | None = None
    error_type: str = "TranscodeError"
    error_message: str
    retryable: bool = True


class ThumbnailGenerated(Event):
    video_id: str
    thumbnail_id: str
    index: int
    status: str
    relative_path: str | None = None
