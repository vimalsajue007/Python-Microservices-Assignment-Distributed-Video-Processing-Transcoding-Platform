"""Unit tests: job creation, retry policy and event handling in the orchestrator.

The metadata service is replaced by an ``httpx.MockTransport`` so these tests need
neither FFmpeg nor the other services.
"""

from __future__ import annotations

from datetime import timedelta

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, update

from helpers.fakes import FakeRedis
from orchestrator_service.config import Settings
from orchestrator_service.main import create_app
from orchestrator_service.models import ProcessingJob
from vp_common.db import utcnow
from vp_common.errors import APIError
from vp_common.events import (
    AttemptFailed, AttemptStarted, AttemptSucceeded, JobProgress, OutputInfo, TranscodeJob,
)
from vp_common.messaging import InMemoryBroker, Message, RejectMessage

VIDEO_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def video_json(**over):
    base = {
        "video_id": VIDEO_ID, "status": "uploaded", "analysis_status": "ok", "height": 1080, "width": 1920,
        "storage_path": f"uploads/{VIDEO_ID}/original.mp4", "error_message": None,
    }
    return {**base, **over}


class Env:
    def __init__(self, tmp_path, video=None, **settings):
        self.video = video if video is not None else video_json()
        self.broker = InMemoryBroker()
        self.broker.auto_pump = False  # we only look at what gets published

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/internal/videos/{VIDEO_ID}":
                if self.video is None:
                    return httpx.Response(404, json={})
                return httpx.Response(200, json=self.video)
            return httpx.Response(404, json={})

        client = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://metadata")
        values = dict(
            database_url=f"sqlite:///{tmp_path / 'orch.db'}", auto_create_schema=True, enable_background=False,
            retry_backoff_base_seconds=0, retry_backoff_max_seconds=0, republish_grace_seconds=0,
        )
        values.update(settings)
        cfg = Settings(**values)
        self.app = create_app(cfg, broker=self.broker, metadata_client=client, redis_client=FakeRedis())
        self.service = self.app.state.service
        self.db = self.app.state.db
        self.http = TestClient(self.app)

    def job(self, job_id) -> ProcessingJob:
        with self.db.session() as s:
            return s.get(ProcessingJob, job_id)

    def started(self, job_id, attempt=1):
        self.service.handle(Message("job.attempt.started", AttemptStarted(job_id=job_id, video_id=VIDEO_ID, attempt=attempt, worker_id="w1").to_body()))

    def failed(self, job_id, attempt=1, retryable=True, error="boom"):
        ev = AttemptFailed(job_id=job_id, video_id=VIDEO_ID, attempt=attempt, error_message=error, retryable=retryable)
        self.service.handle(Message("job.attempt.failed", ev.to_body()))

    def succeeded(self, job_id, attempt=1):
        ev = AttemptSucceeded(
            job_id=job_id, video_id=VIDEO_ID, attempt=attempt, worker_id="w1", duration_ms=10,
            outputs=[OutputInfo(resolution="360p", height=360, width=640, size_bytes=1, relative_path="outputs/x/360p.mp4")],
        )
        self.service.handle(Message("job.attempt.succeeded", ev.to_body()))


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


# --------------------------------------------------------------------------- #
# job creation
# --------------------------------------------------------------------------- #
def test_process_creates_job_and_publishes_work_message(env):
    resp = env.http.post(f"/videos/{VIDEO_ID}/process", json={"resolutions": [720, 360]})
    assert resp.status_code == 202
    job = resp.json()
    assert job["status"] == "queued" and job["progress"] == 0
    assert job["resolutions"] == [360, 720]  # normalised + sorted
    assert job["attempt"] == 1 and job["max_attempts"] == 3 and job["retries_remaining"] == 2

    row = env.job(job["id"])
    assert row.video_id == VIDEO_ID and row.source_path == f"uploads/{VIDEO_ID}/original.mp4"
    assert row.last_published_at is not None

    message = TranscodeJob.model_validate(env.broker.messages("transcode")[0])
    assert (message.job_id, message.video_id, message.attempt, message.resolutions) == (job["id"], VIDEO_ID, 1, [360, 720])
    assert message.source_path == row.source_path

    statuses = env.broker.messages("job.status_changed")
    assert [s["status"] for s in statuses] == ["queued"]  # announced before the work message


def test_default_resolutions_come_from_settings(env):
    job = env.http.post(f"/videos/{VIDEO_ID}/process").json()
    assert job["resolutions"] == [360, 720, 1080]


def test_resolutions_above_source_height_are_dropped(tmp_path):
    e = Env(tmp_path, video=video_json(height=720))
    assert e.http.post(f"/videos/{VIDEO_ID}/process").json()["resolutions"] == [360, 720]


def test_tiny_source_falls_back_to_native_even_height(tmp_path):
    e = Env(tmp_path, video=video_json(height=181))
    assert e.http.post(f"/videos/{VIDEO_ID}/process").json()["resolutions"] == [180]


def test_eligible_heights_helper():
    from orchestrator_service.service import OrchestratorService as S

    assert S.eligible_heights([360, 720, 1080], 1080) == [360, 720, 1080]
    assert S.eligible_heights([360, 720, 1080], 500) == [360]
    assert S.eligible_heights([720, 1080], 240) == [240]
    assert S.eligible_heights([360], None) == [360]


@pytest.mark.parametrize("resolutions", [[999], [360, 5000], [0]])
def test_unsupported_resolutions_are_422(env, resolutions):
    resp = env.http.post(f"/videos/{VIDEO_ID}/process", json={"resolutions": resolutions})
    assert resp.status_code == 422


def test_max_attempts_override_is_validated(env):
    assert env.http.post(f"/videos/{VIDEO_ID}/process", json={"max_attempts": 0}).status_code == 422
    assert env.http.post(f"/videos/{VIDEO_ID}/process", json={"max_attempts": 11}).status_code == 422
    job = env.http.post(f"/videos/{VIDEO_ID}/process", json={"max_attempts": 5}).json()
    assert job["max_attempts"] == 5


def test_unknown_video_is_404(tmp_path):
    e = Env(tmp_path)
    e.video = None
    resp = e.http.post(f"/videos/{VIDEO_ID}/process")
    assert resp.status_code == 404 and e.broker.messages("transcode") == []


def test_video_still_being_analysed_is_409(tmp_path):
    e = Env(tmp_path, video=video_json(analysis_status="pending"))
    resp = e.http.post(f"/videos/{VIDEO_ID}/process")
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "video_not_ready"


def test_undecodable_video_is_422(tmp_path):
    e = Env(tmp_path, video=video_json(status="invalid", analysis_status="failed", error_message="bad"))
    resp = e.http.post(f"/videos/{VIDEO_ID}/process")
    assert resp.status_code == 422 and resp.json()["error"]["code"] == "video_not_processable"


def test_second_active_job_for_same_video_is_409(env):
    first = env.http.post(f"/videos/{VIDEO_ID}/process").json()
    second = env.http.post(f"/videos/{VIDEO_ID}/process")
    assert second.status_code == 409
    assert second.json()["error"]["details"]["job_id"] == first["id"]
    assert len(env.broker.messages("transcode")) == 1


def test_new_job_allowed_once_previous_one_is_finished(env):
    first = env.http.post(f"/videos/{VIDEO_ID}/process").json()
    env.started(first["id"])
    env.succeeded(first["id"])
    assert env.http.post(f"/videos/{VIDEO_ID}/process").status_code == 202


def test_malformed_path_and_body_are_422(env):
    assert env.http.post("/videos/not-a-uuid/process").status_code == 422
    assert env.http.post(f"/videos/{VIDEO_ID}/process", content=b"{nope", headers={"content-type": "application/json"}).status_code == 422


def test_broker_outage_leaves_job_queued_and_scheduler_republishes(env):
    env.broker.fail_publish = True
    resp = env.http.post(f"/videos/{VIDEO_ID}/process")
    assert resp.status_code == 202 and resp.json()["status"] == "queued"  # the request itself still succeeds
    env.broker.fail_publish = False
    with env.db.session() as s:
        orphan = s.scalar(select(ProcessingJob))
    # the job row is durable but unpublished, so the scheduler repairs it
    assert orphan is not None and orphan.last_published_at is None
    stats = env.service.tick()
    assert stats["republished"] == 1
    assert len(env.broker.messages("transcode")) == 1
    assert env.service.tick()["republished"] == 0


# --------------------------------------------------------------------------- #
# progress + retry state machine
# --------------------------------------------------------------------------- #
def test_started_and_progress_events_update_the_job(env):
    job_id = env.http.post(f"/videos/{VIDEO_ID}/process").json()["id"]
    env.started(job_id)
    for pct in (10, 40, 25):  # out-of-order progress must never move backwards
        env.service.handle(Message("job.progress", JobProgress(job_id=job_id, video_id=VIDEO_ID, attempt=1, progress=pct).to_body()))
    row = env.job(job_id)
    assert row.status == "running" and row.progress == 40 and row.started_at is not None
    assert row.attempt_history[0]["status"] == "running" and row.attempt_history[0]["worker_id"] == "w1"


def test_progress_from_a_stale_attempt_is_ignored(env):
    job_id = env.http.post(f"/videos/{VIDEO_ID}/process").json()["id"]
    env.started(job_id)
    env.service.handle(Message("job.progress", JobProgress(job_id=job_id, video_id=VIDEO_ID, attempt=7, progress=90).to_body()))
    assert env.job(job_id).progress == 0


def test_retryable_failure_schedules_a_retry_with_backoff(tmp_path):
    e = Env(tmp_path, retry_backoff_base_seconds=5, retry_backoff_max_seconds=60)
    job_id = e.http.post(f"/videos/{VIDEO_ID}/process").json()["id"]
    e.started(job_id)
    e.failed(job_id, attempt=1, error="ffmpeg crashed")
    row = e.job(job_id)
    assert row.status == "retrying" and "ffmpeg crashed" in row.last_error
    assert 4 < (row.next_retry_at - utcnow()).total_seconds() <= 5
    assert [e.service.backoff_seconds(n) for n in (1, 2, 3, 4, 5, 6)] == [5, 10, 20, 40, 60, 60]  # exponential, capped


def test_scheduler_publishes_next_attempt_when_backoff_elapsed(env):
    job_id = env.http.post(f"/videos/{VIDEO_ID}/process").json()["id"]
    env.started(job_id)
    env.failed(job_id)
    assert env.service.tick()["retried"] == 1
    messages = env.broker.messages("transcode")
    assert [m["attempt"] for m in messages] == [1, 2]
    assert env.job(job_id).status == "queued" and env.job(job_id).current_attempt == 2
    assert env.service.tick()["retried"] == 0  # not retried twice


def test_retry_is_not_due_before_the_backoff_elapsed(tmp_path):
    e = Env(tmp_path, retry_backoff_base_seconds=300, retry_backoff_max_seconds=300)
    job_id = e.http.post(f"/videos/{VIDEO_ID}/process").json()["id"]
    e.started(job_id)
    e.failed(job_id)
    assert e.service.tick()["retried"] == 0
    assert e.service.tick(now=utcnow() + timedelta(seconds=301))["retried"] == 1


def test_attempts_exhausted_marks_job_failed_and_dead_letters_it(env):
    job_id = env.http.post(f"/videos/{VIDEO_ID}/process", json={"max_attempts": 2}).json()["id"]
    env.started(job_id, 1); env.failed(job_id, 1, error="first")
    env.service.tick()
    env.started(job_id, 2); env.failed(job_id, 2, error="second")

    row = env.job(job_id)
    assert row.status == "failed" and row.dead_lettered_at is not None and "second" in row.last_error
    assert [h["status"] for h in row.attempt_history] == ["failed", "failed"]
    dead = env.broker.dead_letters["transcode.jobs"]
    assert len(dead) == 1 and dead[0].body["job_id"] == job_id and dead[0].body["attempts"] == 2
    assert env.service.tick()["retried"] == 0  # failed jobs are never retried automatically


def test_non_retryable_failure_fails_immediately(env):
    job_id = env.http.post(f"/videos/{VIDEO_ID}/process").json()["id"]
    env.started(job_id)
    env.failed(job_id, retryable=False, error="source video not found")
    row = env.job(job_id)
    assert row.status == "failed" and row.current_attempt == 1
    assert env.broker.dead_letters["transcode.jobs"][0].body["reason"] == "non-retryable error"


def test_duplicate_failure_events_are_idempotent(env):
    job_id = env.http.post(f"/videos/{VIDEO_ID}/process").json()["id"]
    env.started(job_id)
    for _ in range(3):
        env.failed(job_id)
    row = env.job(job_id)
    assert row.status == "retrying" and len(row.attempt_history) == 1


def test_success_event_is_idempotent_and_wins_over_late_failure(env):
    job_id = env.http.post(f"/videos/{VIDEO_ID}/process").json()["id"]
    env.started(job_id)
    env.succeeded(job_id)
    env.succeeded(job_id)
    env.failed(job_id)  # late duplicate from a dead attempt
    row = env.job(job_id)
    assert row.status == "completed" and row.progress == 100 and row.completed_at is not None
    assert row.last_error is None and len(row.attempt_history) == 1


def test_stale_running_job_is_failed_by_the_scheduler(env):
    job_id = env.http.post(f"/videos/{VIDEO_ID}/process").json()["id"]
    env.started(job_id)
    with env.db.session() as s:
        s.execute(update(ProcessingJob).where(ProcessingJob.id == job_id).values(heartbeat_at=utcnow() - timedelta(seconds=10_000)))
    assert env.service.tick()["stale"] == 1
    row = env.job(job_id)
    assert row.status == "retrying" and "WorkerTimeout" in row.last_error


def test_manual_retry_requires_failed_job_and_grants_new_budget(env):
    job_id = env.http.post(f"/videos/{VIDEO_ID}/process", json={"max_attempts": 1}).json()["id"]
    assert env.http.post(f"/jobs/{job_id}/retry").status_code == 409  # still queued
    env.started(job_id); env.failed(job_id)
    assert env.job(job_id).status == "failed"

    resp = env.http.post(f"/jobs/{job_id}/retry")
    assert resp.status_code == 202
    row = env.job(job_id)
    assert row.status == "queued" and row.current_attempt == 2 and row.max_attempts == 1 + 3
    assert row.dead_lettered_at is None
    assert [m["attempt"] for m in env.broker.messages("transcode")] == [1, 2]
    assert env.http.post("/jobs/11111111-1111-4111-8111-111111111111/retry").status_code == 404


def test_malformed_events_are_rejected_not_retried(env):
    with pytest.raises(RejectMessage):
        env.service.handle(Message("job.progress", {"job_id": "x"}))
    env.service.handle(Message("unknown.key", {}))  # ignored


def test_events_for_unknown_jobs_are_ignored(env):
    env.started("99999999-9999-4999-8999-999999999999")  # must not raise


def test_get_job_and_list_endpoints(env):
    job = env.http.post(f"/videos/{VIDEO_ID}/process").json()
    assert env.http.get(f"/jobs/{job['id']}").json()["id"] == job["id"]
    assert env.http.get(f"/videos/{VIDEO_ID}/jobs").json()[0]["id"] == job["id"]
    assert env.http.get("/jobs", params={"status": "queued"}).json()[0]["id"] == job["id"]
    assert env.http.get("/jobs/11111111-1111-4111-8111-111111111111").status_code == 404
    assert env.http.get("/jobs/nope").status_code == 422
