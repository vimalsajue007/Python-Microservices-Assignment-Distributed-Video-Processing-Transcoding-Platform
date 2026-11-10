"""Transcoding Orchestrator Service (HTTP API + event consumer + scheduler)."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import httpx
import redis
from fastapi import Body, FastAPI, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy import select

from vp_common.db import Database
from vp_common.errors import UUID_PATTERN, APIError, install_error_handlers
from vp_common.events import JOB_TERMINAL_STATUSES
from vp_common.http import ServiceClient
from vp_common.locks import LockManager
from vp_common.logging import configure_logging
from vp_common.messaging import Broker, RabbitBroker
from vp_common.runtime import PeriodicTask, health_router
from vp_common.topology import Q_ORCHESTRATOR

from .config import Settings
from .models import Base, ProcessingJob
from .service import OrchestratorService

log = logging.getLogger("orchestrator")


class ProcessRequest(BaseModel):
    resolutions: list[int] | None = Field(default=None, description="Heights to produce, e.g. [360, 720, 1080]", max_length=8)
    max_attempts: int | None = Field(default=None, ge=1, le=10, description="Override the retry budget")


class JobOut(BaseModel):
    id: str
    video_id: str
    job_type: str
    status: str
    progress: int
    resolutions: list[int]
    attempt: int
    max_attempts: int
    retries_remaining: int
    last_error: str | None
    attempt_history: list[dict[str, Any]]
    next_retry_at: datetime | None
    dead_lettered: bool
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    @classmethod
    def from_row(cls, j: ProcessingJob) -> "JobOut":
        terminal = j.status in JOB_TERMINAL_STATUSES
        return cls(
            id=j.id, video_id=j.video_id, job_type=j.job_type, status=j.status, progress=j.progress,
            resolutions=list(j.requested_resolutions or []), attempt=j.current_attempt,
            max_attempts=j.max_attempts,
            retries_remaining=0 if terminal else max(j.max_attempts - j.current_attempt, 0),
            last_error=j.last_error, attempt_history=list(j.attempt_history or []),
            next_retry_at=j.next_retry_at, dead_lettered=j.dead_lettered_at is not None,
            created_at=j.created_at, updated_at=j.updated_at, started_at=j.started_at,
            completed_at=j.completed_at,
        )


def create_app(
    settings: Settings | None = None,
    *,
    broker: Broker | None = None,
    metadata_client: httpx.Client | None = None,
    redis_client: Any = None,
) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.log_level, settings.service_name)

    db = Database(settings.database_url or "")
    if settings.auto_create_schema:
        Base.metadata.create_all(db.engine)
    broker = broker or RabbitBroker(
        settings.rabbitmq_url,
        max_redeliveries=settings.consumer_max_redeliveries,
        retry_delay_ms=settings.consumer_retry_delay_ms,
    )
    metadata = ServiceClient(
        settings.metadata_service_url, client=metadata_client, timeout=settings.http_timeout_seconds,
        name="metadata-service",
    )
    if redis_client is None:
        redis_client = redis.Redis.from_url(settings.redis_url, socket_connect_timeout=2, socket_timeout=2)
    service = OrchestratorService(settings, db, broker, metadata, LockManager(redis_client))

    def start_consumers() -> None:
        broker.consume(Q_ORCHESTRATOR, service.handle, prefetch=4)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        tasks: list[PeriodicTask] = []
        if settings.enable_background:
            start_consumers()
            tasks.append(PeriodicTask("scheduler", settings.scheduler_interval_seconds, service.tick).start())
        yield
        for t in tasks:
            t.stop()
        broker.close()
        db.dispose()

    app = FastAPI(title="Transcoding Orchestrator Service", version="1.0.0", lifespan=lifespan)
    app.state.settings, app.state.db, app.state.broker = settings, db, broker
    app.state.service, app.state.start_consumers = service, start_consumers
    install_error_handlers(app)
    app.include_router(health_router(settings.service_name, db))

    @app.post("/videos/{video_id}/process", status_code=202, response_model=JobOut, tags=["jobs"])
    def process_video(video_id: str = Path(pattern=UUID_PATTERN), body: ProcessRequest | None = Body(default=None)):
        """Create a processing job and publish it to RabbitMQ. Returns immediately (202)."""
        body = body or ProcessRequest()
        job = service.create_job(video_id, body.resolutions, body.max_attempts)
        with db.session() as s:  # reload: the worker may already have advanced the state
            return JobOut.from_row(s.get(ProcessingJob, job.id))

    @app.get("/videos/{video_id}/jobs", response_model=list[JobOut], tags=["jobs"])
    def list_video_jobs(video_id: str = Path(pattern=UUID_PATTERN)):
        with db.session() as s:
            rows = s.scalars(
                select(ProcessingJob).where(ProcessingJob.video_id == video_id).order_by(ProcessingJob.created_at.desc())
            ).all()
            return [JobOut.from_row(r) for r in rows]

    @app.get("/jobs", response_model=list[JobOut], tags=["jobs"])
    def list_jobs(status: str | None = Query(default=None), limit: int = Query(default=50, ge=1, le=200)):
        stmt = select(ProcessingJob)
        if status:
            stmt = stmt.where(ProcessingJob.status == status)
        with db.session() as s:
            rows = s.scalars(stmt.order_by(ProcessingJob.created_at.desc()).limit(limit)).all()
            return [JobOut.from_row(r) for r in rows]

    @app.get("/jobs/{job_id}", response_model=JobOut, tags=["jobs"])
    def get_job(job_id: str = Path(pattern=UUID_PATTERN)):
        with db.session() as s:
            row = s.get(ProcessingJob, job_id)
            if row is None:
                raise APIError(404, "job_not_found", f"Job {job_id} not found")
            return JobOut.from_row(row)

    @app.post("/jobs/{job_id}/retry", status_code=202, response_model=JobOut, tags=["jobs"])
    def retry_job(job_id: str = Path(pattern=UUID_PATTERN)):
        """Re-queue a failed job with a fresh retry budget."""
        job = service.retry_job(job_id)
        with db.session() as s:
            return JobOut.from_row(s.get(ProcessingJob, job.id))

    return app
