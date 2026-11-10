"""Orchestration logic: job creation, progress tracking, retries, dead-lettering.

State machine::

    queued --attempt.started--> running --attempt.succeeded--> completed
       ^                           |
       |                           +--attempt.failed--> retrying --(backoff elapsed)--> queued
       |                           |                                   ^
       +---------------------------+                                   |
                                   +--attempt.failed (budget spent)--> failed + dead-letter

Rules that keep the flow safe under at-least-once delivery:

* every state change is guarded by the attempt number, so duplicated or late
  events are ignored;
* a job row is locked (``SELECT ... FOR UPDATE``) while it is modified;
* events are always published *after* the transaction commits;
* a scheduler repairs the gaps (unpublished jobs, due retries, dead workers).
"""

from __future__ import annotations

import logging
from contextlib import nullcontext
from datetime import timedelta

from pydantic import ValidationError
from sqlalchemy import select, update

from vp_common.db import Database, new_id, utcnow
from vp_common.errors import APIError
from vp_common.events import (
    ALLOWED_RESOLUTIONS, JOB_ACTIVE_STATUSES, JOB_COMPLETED, JOB_FAILED, JOB_QUEUED,
    JOB_RETRYING, JOB_RUNNING, JOB_TERMINAL_STATUSES,
    AttemptFailed, AttemptStarted, AttemptSucceeded, JobProgress, JobStatusChanged, TranscodeJob,
)
from vp_common.http import ServiceClient
from vp_common.locks import LockBusy, LockManager
from vp_common.messaging import Broker, BrokerError, Message, RejectMessage
from vp_common.topology import (
    EX_DLX, EX_EVENTS, EX_JOBS, Q_TRANSCODE,
    RK_JOB_FAILED, RK_JOB_PROGRESS, RK_JOB_STARTED, RK_JOB_STATUS, RK_JOB_SUCCEEDED, RK_TRANSCODE,
)

from .config import Settings
from .models import ProcessingJob

log = logging.getLogger("orchestrator")


def _iso(dt) -> str:
    return dt.isoformat()


def _upsert_history(history: list[dict] | None, attempt: int, **fields) -> list[dict]:
    """Return a new list (so SQLAlchemy detects the JSON change) with the attempt entry updated."""
    items = [dict(h) for h in (history or [])]
    for entry in items:
        if entry.get("attempt") == attempt:
            entry.update({k: v for k, v in fields.items() if v is not None or k in ("error",)})
            return items
    items.append({"attempt": attempt, **fields})
    return items


class OrchestratorService:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        broker: Broker,
        metadata: ServiceClient,
        locks: LockManager | None = None,
    ):
        self.settings = settings
        self.db = db
        self.broker = broker
        self.metadata = metadata
        self.locks = locks

    # ------------------------------------------------------------------ #
    # Job creation (POST /videos/{id}/process)
    # ------------------------------------------------------------------ #
    def normalize_resolutions(self, requested: list[int] | None) -> list[int]:
        values = requested if requested else self.settings.default_resolution_list
        bad = [v for v in values if v not in ALLOWED_RESOLUTIONS]
        if bad:
            raise APIError(
                422, "invalid_resolution",
                f"Unsupported resolution(s) {bad}. Allowed: {list(ALLOWED_RESOLUTIONS)}",
            )
        return sorted(set(values))

    @staticmethod
    def eligible_heights(requested: list[int], source_height: int | None) -> list[int]:
        """Never upscale. If the source is smaller than every target, keep its native height."""
        if not source_height:
            return requested
        eligible = [h for h in requested if h <= source_height]
        if eligible:
            return eligible
        native = source_height - (source_height % 2)  # libx264 needs even dimensions
        return [max(native, 2)]

    def create_job(self, video_id: str, resolutions: list[int] | None = None, max_attempts: int | None = None) -> ProcessingJob:
        requested = self.normalize_resolutions(resolutions)
        video = self.metadata.get_json(f"/internal/videos/{video_id}")  # 404 -> APIError
        if video.get("status") == "invalid" or video.get("analysis_status") == "failed":
            raise APIError(422, "video_not_processable", f"Video is not a decodable media file: {video.get('error_message')}")
        if video.get("analysis_status") != "ok":
            raise APIError(409, "video_not_ready", "Video is still being analysed; retry in a moment")

        heights = self.eligible_heights(requested, video.get("height"))
        owner = new_id()
        guard = self.locks.hold(f"process:{video_id}", owner, 30) if self.locks else nullcontext()
        try:
            with guard:
                with self.db.session() as s:
                    active = s.scalar(
                        select(ProcessingJob).where(
                            ProcessingJob.video_id == video_id, ProcessingJob.status.in_(JOB_ACTIVE_STATUSES)
                        )
                    )
                    if active is not None:
                        raise APIError(
                            409, "job_in_progress",
                            "This video already has an active processing job",
                            details={"job_id": active.id, "status": active.status},
                        )
                    job = ProcessingJob(
                        video_id=video_id, status=JOB_QUEUED, progress=0,
                        requested_resolutions=heights, source_path=video["storage_path"],
                        max_attempts=max_attempts or self.settings.max_attempts,
                        current_attempt=1, attempt_history=[],
                    )
                    s.add(job)
        except LockBusy as exc:
            raise APIError(409, "job_in_progress", "A processing request for this video is already being handled") from exc

        # Causal order: status first, then the work message (the worker may finish instantly).
        self._publish_status(job)
        self._dispatch(job)
        return job

    # ------------------------------------------------------------------ #
    # Manual retry of a failed job (POST /jobs/{id}/retry)
    # ------------------------------------------------------------------ #
    def retry_job(self, job_id: str) -> ProcessingJob:
        with self.db.session() as s:
            job = s.get(ProcessingJob, job_id, with_for_update=True)
            if job is None:
                raise APIError(404, "job_not_found", f"Job {job_id} not found")
            if job.status != JOB_FAILED:
                raise APIError(409, "job_not_failed", f"Only failed jobs can be retried (status is '{job.status}')")
            other = s.scalar(
                select(ProcessingJob).where(
                    ProcessingJob.video_id == job.video_id,
                    ProcessingJob.status.in_(JOB_ACTIVE_STATUSES),
                    ProcessingJob.id != job.id,
                )
            )
            if other is not None:
                raise APIError(409, "job_in_progress", "Another job is already active for this video",
                               details={"job_id": other.id})
            job.max_attempts += self.settings.max_attempts  # grant a fresh retry budget
            job.current_attempt += 1
            job.status = JOB_QUEUED
            job.progress = 0
            job.completed_at = None
            job.dead_lettered_at = None
            job.next_retry_at = None
            job.last_published_at = None
        self._publish_status(job)
        self._dispatch(job)
        return job

    # ------------------------------------------------------------------ #
    # Publishing helpers
    # ------------------------------------------------------------------ #
    def _dispatch(self, job: ProcessingJob) -> bool:
        message = TranscodeJob(
            job_id=job.id, video_id=job.video_id, source_path=job.source_path,
            resolutions=list(job.requested_resolutions), attempt=job.current_attempt,
            max_attempts=job.max_attempts,
        )
        try:
            self.broker.publish(EX_JOBS, RK_TRANSCODE, message.to_body(), message_id=message.message_id)
        except BrokerError as exc:
            log.warning("job %s attempt %s not published (%s); scheduler will retry", job.id, job.current_attempt, exc)
            return False
        with self.db.session() as s:
            row = s.get(ProcessingJob, job.id, with_for_update=True)
            if row is not None and row.status == JOB_QUEUED:
                row.last_published_at = utcnow()
        return True

    def _publish_status(self, job: ProcessingJob, *, best_effort: bool = True) -> None:
        """Announce the job state.

        ``best_effort=True`` (API / scheduler paths): a broker outage is logged, the
        durable job row lets the scheduler repair the gap. ``False`` (event handlers):
        the error propagates so the consumer redelivers the triggering event, whose
        handler is idempotent and re-announces the state.
        """
        error = job.last_error if job.status in (JOB_RETRYING, JOB_FAILED) else None
        event = JobStatusChanged(
            job_id=job.id, video_id=job.video_id, status=job.status, progress=job.progress,
            attempt=job.current_attempt, max_attempts=job.max_attempts, error_message=error,
        )
        try:
            self.broker.publish(EX_EVENTS, RK_JOB_STATUS, event.to_body())
        except BrokerError as exc:
            if not best_effort:
                raise
            log.warning("status event for job %s not published: %s", job.id, exc)

    def _dead_letter(self, job: ProcessingJob, reason: str) -> None:
        payload = {
            "job_id": job.id, "video_id": job.video_id, "reason": reason,
            "attempts": job.current_attempt, "last_error": job.last_error,
            "message": TranscodeJob(
                job_id=job.id, video_id=job.video_id, source_path=job.source_path,
                resolutions=list(job.requested_resolutions), attempt=job.current_attempt,
                max_attempts=job.max_attempts,
            ).to_body(),
        }
        try:
            self.broker.publish(EX_DLX, Q_TRANSCODE.name, payload)
        except BrokerError as exc:
            log.error("could not dead-letter job %s: %s", job.id, exc)

    def backoff_seconds(self, attempt: int) -> float:
        s = self.settings
        return min(s.retry_backoff_base_seconds * (2 ** max(attempt - 1, 0)), s.retry_backoff_max_seconds)

    # ------------------------------------------------------------------ #
    # Event handling (queue: orchestrator.events)
    # ------------------------------------------------------------------ #
    def handle(self, msg: Message) -> None:
        try:
            if msg.routing_key == RK_JOB_STARTED:
                job = self._on_started(AttemptStarted.model_validate(msg.body))
            elif msg.routing_key == RK_JOB_PROGRESS:
                job = self._on_progress(JobProgress.model_validate(msg.body))
            elif msg.routing_key == RK_JOB_SUCCEEDED:
                job = self._on_succeeded(AttemptSucceeded.model_validate(msg.body))
            elif msg.routing_key == RK_JOB_FAILED:
                job = self._on_failed(AttemptFailed.model_validate(msg.body))
            else:
                log.warning("ignoring unexpected routing key %s", msg.routing_key)
                return
        except ValidationError as exc:
            raise RejectMessage(f"invalid {msg.routing_key} payload: {exc.errors()[:2]}") from exc
        if job is not None:
            self._publish_status(job, best_effort=False)

    def _on_started(self, ev: AttemptStarted) -> ProcessingJob | None:
        now = utcnow()
        with self.db.session() as s:
            job = s.get(ProcessingJob, ev.job_id, with_for_update=True)
            if job is None:
                log.warning("attempt.started for unknown job %s", ev.job_id)
                return None
            if job.status in JOB_TERMINAL_STATUSES:
                return job  # duplicate/late: re-announce current state
            if ev.attempt < job.current_attempt:
                return None
            job.current_attempt = ev.attempt
            job.status = JOB_RUNNING
            job.progress = 0
            job.heartbeat_at = now
            job.started_at = job.started_at or now
            job.next_retry_at = None
            job.attempt_history = _upsert_history(
                job.attempt_history, ev.attempt, status="running", worker_id=ev.worker_id, started_at=_iso(now)
            )
        return job

    def _on_progress(self, ev: JobProgress) -> ProcessingJob | None:
        now = utcnow()
        with self.db.session() as s:
            job = s.get(ProcessingJob, ev.job_id, with_for_update=True)
            if job is None or job.status in JOB_TERMINAL_STATUSES or ev.attempt != job.current_attempt:
                return None
            job.status = JOB_RUNNING
            job.progress = max(job.progress, min(ev.progress, 99))
            job.heartbeat_at = now
        return job

    def _on_succeeded(self, ev: AttemptSucceeded) -> ProcessingJob | None:
        now = utcnow()
        with self.db.session() as s:
            job = s.get(ProcessingJob, ev.job_id, with_for_update=True)
            if job is None:
                log.warning("attempt.succeeded for unknown job %s", ev.job_id)
                return None
            if job.status == JOB_COMPLETED:
                return job  # duplicate delivery
            if ev.attempt < job.current_attempt:
                return None
            job.status = JOB_COMPLETED
            job.progress = 100
            job.completed_at = now
            job.heartbeat_at = now
            job.next_retry_at = None
            job.dead_lettered_at = None
            job.last_error = None
            job.current_attempt = ev.attempt
            job.attempt_history = _upsert_history(
                job.attempt_history, ev.attempt, status="succeeded", worker_id=ev.worker_id,
                finished_at=_iso(now), duration_ms=ev.duration_ms, error=None,
            )
        return job

    def _on_failed(self, ev: AttemptFailed) -> ProcessingJob | None:
        return self._register_failure(
            ev.job_id, ev.attempt, f"{ev.error_type}: {ev.error_message}"[:4000], ev.retryable, ev.worker_id
        )

    def _register_failure(
        self, job_id: str, attempt: int, error: str, retryable: bool, worker_id: str | None = None
    ) -> ProcessingJob | None:
        now = utcnow()
        newly_failed = False
        with self.db.session() as s:
            job = s.get(ProcessingJob, job_id, with_for_update=True)
            if job is None:
                log.warning("attempt.failed for unknown job %s", job_id)
                return None
            if job.status == JOB_COMPLETED:
                return job
            if attempt < job.current_attempt:
                return None
            if any(h.get("attempt") == attempt and h.get("status") == "failed" for h in (job.attempt_history or [])):
                return job  # duplicate delivery

            job.attempt_history = _upsert_history(
                job.attempt_history, attempt, status="failed", worker_id=worker_id,
                finished_at=_iso(now), error=error,
            )
            job.last_error = error
            job.heartbeat_at = now
            if attempt >= job.max_attempts or not retryable:
                job.status = JOB_FAILED
                job.completed_at = now
                job.dead_lettered_at = now
                job.next_retry_at = None
                newly_failed = True
            else:
                job.status = JOB_RETRYING
                job.next_retry_at = now + timedelta(seconds=self.backoff_seconds(attempt))
        if newly_failed:
            reason = "attempts exhausted" if retryable else "non-retryable error"
            self._dead_letter(job, reason)
        return job

    # ------------------------------------------------------------------ #
    # Scheduler: repairs gaps and drives retries
    # ------------------------------------------------------------------ #
    def tick(self, now=None) -> dict[str, int]:
        now = now or utcnow()
        stats = {"retried": 0, "republished": 0, "stale": 0}

        # 1) retries whose backoff elapsed
        with self.db.session() as s:
            due = list(
                s.scalars(
                    select(ProcessingJob.id).where(
                        ProcessingJob.status == JOB_RETRYING, ProcessingJob.next_retry_at <= now
                    )
                )
            )
        for job_id in due:
            with self.db.session() as s:
                claimed = s.execute(
                    update(ProcessingJob)
                    .where(ProcessingJob.id == job_id, ProcessingJob.status == JOB_RETRYING)
                    .values(
                        status=JOB_QUEUED, current_attempt=ProcessingJob.current_attempt + 1,
                        next_retry_at=None, last_published_at=None, progress=0, updated_at=now,
                    )
                ).rowcount
                job = s.get(ProcessingJob, job_id) if claimed else None
            if job is not None:
                self._publish_status(job)
                self._dispatch(job)
                stats["retried"] += 1

        # 2) queued jobs whose work message never reached the broker
        grace = now - timedelta(seconds=self.settings.republish_grace_seconds)
        with self.db.session() as s:
            orphans = list(
                s.scalars(
                    select(ProcessingJob).where(
                        ProcessingJob.status == JOB_QUEUED,
                        ProcessingJob.last_published_at.is_(None),
                        ProcessingJob.updated_at < grace,
                    )
                )
            )
        for job in orphans:
            if self._dispatch(job):
                stats["republished"] += 1

        # 3) running jobs whose worker went silent
        stale_before = now - timedelta(seconds=self.settings.job_stale_seconds)
        with self.db.session() as s:
            stale = list(
                s.execute(
                    select(ProcessingJob.id, ProcessingJob.current_attempt).where(
                        ProcessingJob.status == JOB_RUNNING, ProcessingJob.heartbeat_at < stale_before
                    )
                ).all()
            )
        for job_id, attempt in stale:
            job = self._register_failure(
                job_id, attempt,
                f"WorkerTimeout: no heartbeat from the worker for {self.settings.job_stale_seconds}s", True,
            )
            if job is not None:
                self._publish_status(job)
                stats["stale"] += 1
        return stats
