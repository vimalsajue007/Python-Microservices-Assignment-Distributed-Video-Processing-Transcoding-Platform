"""Transcoding worker: consumes ``transcode.jobs`` and runs FFmpeg.

Duplicate-delivery safety (RabbitMQ is at-least-once):

1. A Redis lock ``transcode:<job>:<attempt>`` keeps two workers from running the
   same attempt concurrently (renewed while FFmpeg reports progress).
2. A unique ``(job_id, attempt_number)`` row in ``processing_attempts`` is the
   durable guard: a message for an attempt that already finished is *not*
   processed again - its recorded result is simply re-announced, which also
   heals a lost result event.
3. A ``running`` row without a live lock means the previous worker died; the
   attempt is taken over.
"""

from __future__ import annotations

import logging
import os
import socket
import time
from dataclasses import dataclass
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from vp_common import ffmpeg as ff
from vp_common.db import Database, new_id, utcnow
from vp_common.events import (
    AttemptFailed, AttemptStarted, AttemptSucceeded, JobProgress, OutputInfo, TranscodeJob,
)
from vp_common.locks import LockManager, LockUnavailable
from vp_common.messaging import Broker, Message, RejectMessage
from vp_common.storage import Storage
from vp_common.topology import EX_EVENTS, RK_JOB_FAILED, RK_JOB_PROGRESS, RK_JOB_STARTED, RK_JOB_SUCCEEDED

from .config import Settings
from .models import (
    ATTEMPT_FAILED, ATTEMPT_RUNNING, ATTEMPT_SUCCEEDED, ProcessingAttempt, TranscodingOutput,
)

log = logging.getLogger("transcoder")


class TranscodeError(Exception):
    def __init__(self, message: str, *, retryable: bool = True):
        super().__init__(message)
        self.retryable = retryable


@dataclass
class Claim:
    action: str  # run | skip | reemit
    lock_key: str = ""
    lock_owner: str = ""
    attempt_status: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    duration_ms: int | None = None


class TranscodeWorker:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        broker: Broker,
        storage: Storage,
        locks: LockManager,
        *,
        probe_fn=ff.probe,
        transcode_fn=ff.transcode,
        worker_id: str | None = None,
    ):
        self.settings = settings
        self.db = db
        self.broker = broker
        self.storage = storage
        self.locks = locks
        self.probe = probe_fn
        self.transcode = transcode_fn
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"

    # ------------------------------------------------------------------ #
    def handle(self, msg: Message) -> None:
        try:
            job = TranscodeJob.model_validate(msg.body)
        except ValidationError as exc:
            self._report_poison(msg)
            raise RejectMessage(f"malformed transcode job: {exc.errors()[:2]}") from exc

        claim = self._claim(job, msg)
        if claim.action == "skip":
            log.info("duplicate delivery of job=%s attempt=%s skipped", job.job_id, job.attempt)
            return
        if claim.action == "reemit":
            log.info("job=%s attempt=%s already %s; re-announcing result", job.job_id, job.attempt, claim.attempt_status)
            self._reemit(job, claim)
            return
        try:
            self._run(job, claim)
        finally:
            self.locks.release(claim.lock_key, claim.lock_owner)

    # ------------------------------------------------------------------ #
    def _report_poison(self, msg: Message) -> None:
        body = msg.body if isinstance(msg.body, dict) else {}
        job_id, video_id, attempt = body.get("job_id"), body.get("video_id"), body.get("attempt")
        if not (job_id and video_id and isinstance(attempt, int) and attempt >= 1):
            return
        try:
            self._publish(
                RK_JOB_FAILED,
                AttemptFailed(
                    job_id=job_id, video_id=video_id, attempt=attempt, worker_id=self.worker_id,
                    error_type="InvalidMessage", error_message="Job message was malformed and dead-lettered",
                    retryable=False,
                ),
            )
        except Exception:  # noqa: BLE001
            log.exception("could not report poison message")

    def _publish(self, routing_key: str, event) -> None:
        self.broker.publish(EX_EVENTS, routing_key, event.to_body())

    # ------------------------------------------------------------------ #
    # Idempotency
    # ------------------------------------------------------------------ #
    def _claim(self, job: TranscodeJob, msg: Message) -> Claim:
        key = f"transcode:{job.job_id}:{job.attempt}"
        owner = new_id()
        lock_ok = True
        try:
            if not self.locks.acquire(key, owner, self.settings.lock_ttl_seconds):
                return Claim("skip")
        except LockUnavailable:
            lock_ok = False
            log.warning("redis unavailable; relying on the database guard only")

        claim: Claim | None = None
        try:
            claim = self._claim_in_db(job, msg, key, owner, lock_ok)
        except IntegrityError:
            claim = Claim("skip")  # another worker inserted the attempt first
        finally:
            if claim is None or claim.action != "run":
                self.locks.release(key, owner)
        return claim

    def _claim_in_db(self, job: TranscodeJob, msg: Message, key: str, owner: str, lock_ok: bool) -> Claim:
        now = utcnow()
        with self.db.session() as s:
            row = s.scalar(
                select(ProcessingAttempt)
                .where(ProcessingAttempt.job_id == job.job_id, ProcessingAttempt.attempt_number == job.attempt)
                .with_for_update()
            )
            if row is None:
                s.add(
                    ProcessingAttempt(
                        job_id=job.job_id, video_id=job.video_id, attempt_number=job.attempt,
                        status=ATTEMPT_RUNNING, worker_id=self.worker_id,
                        message_id=job.message_id or msg.message_id, started_at=now,
                    )
                )
                s.flush()  # raises IntegrityError if a concurrent worker won the race
                return Claim("run", key, owner)
            if row.status in (ATTEMPT_SUCCEEDED, ATTEMPT_FAILED):
                return Claim(
                    "reemit", attempt_status=row.status, error_type=row.error_type,
                    error_message=row.error_message, duration_ms=row.duration_ms,
                )
            # status == running: the previous worker is gone if we could take the lock,
            # or (no Redis) if the attempt has been running implausibly long.
            age = (now - row.started_at).total_seconds()
            if lock_ok or age > self.settings.attempt_stale_seconds:
                log.warning("taking over abandoned attempt job=%s attempt=%s", job.job_id, job.attempt)
                row.worker_id = self.worker_id
                row.started_at = now
                return Claim("run", key, owner)
            return Claim("skip")

    # ------------------------------------------------------------------ #
    # Execution
    # ------------------------------------------------------------------ #
    def _run(self, job: TranscodeJob, claim: Claim) -> None:
        t0 = time.monotonic()
        self._publish(
            RK_JOB_STARTED,
            AttemptStarted(job_id=job.job_id, video_id=job.video_id, attempt=job.attempt, worker_id=self.worker_id),
        )
        temp_files: list[Path] = []
        try:
            if self.settings.simulate_failure_attempts and job.attempt <= self.settings.simulate_failure_attempts:
                raise TranscodeError(f"Simulated failure on attempt {job.attempt}")
            source = self.storage.resolve(job.source_path)
            if not source.exists():
                raise TranscodeError(f"source video not found: {job.source_path}", retryable=False)
            duration = self.probe(source, ffprobe_bin=self.settings.ffprobe_bin).duration
            finals = self._encode_all(job, claim, source, duration, temp_files)
        except (TranscodeError, ff.FFmpegError, OSError) as exc:
            for tmp in temp_files:
                tmp.unlink(missing_ok=True)
            self._finish_failed(job, exc, int((time.monotonic() - t0) * 1000))
            return
        self._finish_succeeded(job, finals, int((time.monotonic() - t0) * 1000))

    def _encode_all(self, job: TranscodeJob, claim: Claim, source: Path, duration: float, temp_files: list[Path]):
        total = len(job.resolutions)
        state = {"pct": -1, "t": 0.0}
        encoded: list[tuple[int, Path, Path, str]] = []

        for index, height in enumerate(job.resolutions):
            relative = Storage.rel_output(job.video_id, height)
            final = self.storage.prepare(relative)
            tmp = final.with_name(f".{job.job_id}.{job.attempt}.{height}p.tmp")
            temp_files.append(tmp)

            def on_progress(fraction: float, index=index, height=height) -> None:
                self._progress(job, claim, state, (index + fraction) / total, height)

            self.transcode(
                source, tmp, height,
                duration=duration, on_progress=on_progress,
                ffmpeg_bin=self.settings.ffmpeg_bin, preset=self.settings.ffmpeg_preset,
                crf=self.settings.ffmpeg_crf, timeout=self.settings.transcode_timeout_seconds,
            )
            if not tmp.exists() or tmp.stat().st_size == 0:
                raise TranscodeError(f"ffmpeg produced no output for {height}p")
            encoded.append((height, tmp, final, relative))

        # Publish renditions only after every resolution succeeded.
        results: list[tuple[int, Path, str]] = []
        for height, tmp, final, relative in encoded:
            os.replace(tmp, final)
            results.append((height, final, relative))
        return results

    def _progress(self, job: TranscodeJob, claim: Claim, state: dict, fraction: float, height: int) -> None:
        pct = min(int(fraction * 100), 99)
        now = time.monotonic()
        if pct <= state["pct"]:
            return
        if pct - state["pct"] < self.settings.progress_min_delta and now - state["t"] < self.settings.progress_min_interval_seconds:
            return
        state["pct"], state["t"] = pct, now
        self.locks.extend(claim.lock_key, claim.lock_owner, self.settings.lock_ttl_seconds)
        try:
            self._publish(
                RK_JOB_PROGRESS,
                JobProgress(job_id=job.job_id, video_id=job.video_id, attempt=job.attempt, progress=pct, current_resolution=height),
            )
        except Exception:  # noqa: BLE001 - progress is best effort
            log.warning("could not publish progress for job %s", job.job_id)

    # ------------------------------------------------------------------ #
    # Outcomes
    # ------------------------------------------------------------------ #
    def _finish_succeeded(self, job: TranscodeJob, finals: list[tuple[int, Path, str]], duration_ms: int) -> None:
        infos: list[OutputInfo] = []
        for height, path, relative in finals:
            try:
                probed = self.probe(path, ffprobe_bin=self.settings.ffprobe_bin)
                width = probed.width
            except ff.FFmpegError:
                width = 0
            infos.append(OutputInfo(resolution=f"{height}p", height=height, width=width, size_bytes=path.stat().st_size, relative_path=relative))

        now = utcnow()
        with self.db.session() as s:
            attempt = s.scalar(
                select(ProcessingAttempt)
                .where(ProcessingAttempt.job_id == job.job_id, ProcessingAttempt.attempt_number == job.attempt)
                .with_for_update()
            )
            attempt.status = ATTEMPT_SUCCEEDED
            attempt.finished_at = now
            attempt.duration_ms = duration_ms
            attempt.error_type = attempt.error_message = None
            s.execute(
                delete(TranscodingOutput).where(
                    TranscodingOutput.video_id == job.video_id,
                    TranscodingOutput.height.in_([i.height for i in infos]),
                )
            )
            s.flush()
            for info in infos:
                s.add(
                    TranscodingOutput(
                        video_id=job.video_id, job_id=job.job_id, attempt_id=attempt.id,
                        resolution=info.resolution, height=info.height, width=info.width,
                        size_bytes=info.size_bytes, relative_path=info.relative_path,
                    )
                )
        self._publish(
            RK_JOB_SUCCEEDED,
            AttemptSucceeded(
                job_id=job.job_id, video_id=job.video_id, attempt=job.attempt,
                worker_id=self.worker_id, duration_ms=duration_ms, outputs=infos,
            ),
        )
        log.info("job=%s attempt=%s succeeded (%s) in %dms", job.job_id, job.attempt, [i.resolution for i in infos], duration_ms)

    def _finish_failed(self, job: TranscodeJob, exc: Exception, duration_ms: int) -> None:
        retryable = bool(getattr(exc, "retryable", True))
        error_type = type(exc).__name__
        message = str(exc)[:4000]
        with self.db.session() as s:
            attempt = s.scalar(
                select(ProcessingAttempt)
                .where(ProcessingAttempt.job_id == job.job_id, ProcessingAttempt.attempt_number == job.attempt)
                .with_for_update()
            )
            attempt.status = ATTEMPT_FAILED
            attempt.finished_at = utcnow()
            attempt.duration_ms = duration_ms
            attempt.error_type = error_type
            attempt.error_message = message
        log.warning("job=%s attempt=%s failed: %s", job.job_id, job.attempt, message)
        self._publish(
            RK_JOB_FAILED,
            AttemptFailed(
                job_id=job.job_id, video_id=job.video_id, attempt=job.attempt, worker_id=self.worker_id,
                error_type=error_type, error_message=message, retryable=retryable,
            ),
        )

    def _reemit(self, job: TranscodeJob, claim: Claim) -> None:
        if claim.attempt_status == ATTEMPT_FAILED:
            self._publish(
                RK_JOB_FAILED,
                AttemptFailed(
                    job_id=job.job_id, video_id=job.video_id, attempt=job.attempt, worker_id=self.worker_id,
                    error_type=claim.error_type or "TranscodeError",
                    error_message=claim.error_message or "attempt failed", retryable=True,
                ),
            )
            return
        with self.db.session() as s:
            rows = s.scalars(select(TranscodingOutput).where(TranscodingOutput.job_id == job.job_id)).all()
            infos = [
                OutputInfo(resolution=r.resolution, height=r.height, width=r.width, size_bytes=r.size_bytes, relative_path=r.relative_path)
                for r in rows
            ]
        self._publish(
            RK_JOB_SUCCEEDED,
            AttemptSucceeded(
                job_id=job.job_id, video_id=job.video_id, attempt=job.attempt, worker_id=self.worker_id,
                duration_ms=claim.duration_ms or 0, outputs=infos,
            ),
        )
