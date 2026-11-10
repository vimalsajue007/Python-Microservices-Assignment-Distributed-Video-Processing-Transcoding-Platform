"""Unit tests: transcoding worker - duplicate messages, failures, crash recovery.

FFmpeg is replaced by fakes here so the idempotency rules can be tested fast and
deterministically. Real FFmpeg is covered by tests/integration/test_pipeline.py.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select, update

from helpers.fakes import FakeRedis
from transcoder_service.config import Settings
from transcoder_service.main import create_app
from transcoder_service.models import ProcessingAttempt, TranscodingOutput
from transcoder_service.worker import TranscodeWorker
from vp_common.db import utcnow
from vp_common.ffmpeg import FFmpegError, ProbeResult
from vp_common.messaging import InMemoryBroker, Message, RejectMessage

JOB_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
VIDEO_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


class Harness:
    def __init__(self, tmp_path, **settings):
        self.broker = InMemoryBroker()
        self.redis = FakeRedis()
        self.encoded: list[tuple[int, str]] = []
        self.fail_with: Exception | None = None
        self.probe_error: Exception | None = None

        def fake_probe(path, **kw):
            if self.probe_error:
                raise self.probe_error
            return ProbeResult(duration=4.0, width=1280, height=720, container="mp4", video_codec="h264",
                               audio_codec="aac", bit_rate=1000, frame_rate=25.0, size_bytes=1)

        def fake_transcode(source, dest, height, *, on_progress=None, **kw):
            if self.fail_with:
                raise self.fail_with
            self.encoded.append((height, str(dest)))
            Path(dest).write_bytes(b"x" * (height * 10))
            if on_progress:
                for frac in (0.25, 0.5, 1.0):
                    on_progress(frac)

        values = dict(database_url=f"sqlite:///{tmp_path / 'tc.db'}", auto_create_schema=True,
                      enable_background=False, storage_root=str(tmp_path / "media"),
                      progress_min_delta=1, progress_min_interval_seconds=0)
        values.update(settings)
        self.app = create_app(Settings(**values), broker=self.broker, redis_client=self.redis,
                              probe_fn=fake_probe, transcode_fn=fake_transcode)
        self.worker: TranscodeWorker = self.app.state.worker
        self.db = self.app.state.db
        self.storage = self.worker.storage
        src = self.storage.prepare(f"uploads/{VIDEO_ID}/original.mp4")
        src.write_bytes(b"source")

    def message(self, attempt=1, resolutions=(360, 720), **over) -> Message:
        body = {"message_id": "m-1", "job_id": JOB_ID, "video_id": VIDEO_ID,
                "source_path": f"uploads/{VIDEO_ID}/original.mp4", "resolutions": list(resolutions),
                "attempt": attempt, "max_attempts": 3, **over}
        return Message("transcode", body, message_id="m-1")

    def events(self, key):
        return self.broker.messages(key)

    def attempts(self):
        with self.db.session() as s:
            return list(s.scalars(select(ProcessingAttempt).order_by(ProcessingAttempt.attempt_number)))

    def outputs(self):
        with self.db.session() as s:
            return list(s.scalars(select(TranscodingOutput).order_by(TranscodingOutput.height)))


@pytest.fixture
def h(tmp_path):
    return Harness(tmp_path)


# --------------------------------------------------------------------------- #
def test_successful_job_records_attempt_outputs_and_events(h):
    h.worker.handle(h.message())

    (attempt,) = h.attempts()
    assert attempt.status == "succeeded" and attempt.attempt_number == 1 and attempt.finished_at is not None
    assert [(o.resolution, o.height) for o in h.outputs()] == [("360p", 360), ("720p", 720)]
    assert all((h.storage.root / o.relative_path).exists() for o in h.outputs())
    assert not list((h.storage.root / "outputs").rglob("*.tmp"))  # temp files are promoted

    assert len(h.events("job.attempt.started")) == 1
    progress = [e["progress"] for e in h.events("job.progress")]
    assert progress == sorted(progress) and 0 < progress[0] and progress[-1] <= 99
    (done,) = h.events("job.attempt.succeeded")
    assert [o["resolution"] for o in done["outputs"]] == ["360p", "720p"] and done["attempt"] == 1
    assert h.events("job.attempt.failed") == []


def test_duplicate_delivery_after_success_does_not_reprocess(h):
    h.worker.handle(h.message())
    h.encoded.clear()
    for _ in range(3):
        h.worker.handle(h.message())

    assert h.encoded == []  # FFmpeg did not run again
    assert len(h.attempts()) == 1 and len(h.outputs()) == 2
    assert len(h.events("job.attempt.started")) == 1
    # the recorded result is re-announced (heals a lost result event); the orchestrator ignores duplicates
    assert len(h.events("job.attempt.succeeded")) == 4


def test_duplicate_delivery_after_failure_does_not_rerun_and_reannounces_failure(h):
    h.fail_with = FFmpegError("encoder exploded")
    h.worker.handle(h.message())
    h.fail_with = None
    h.worker.handle(h.message())

    assert h.encoded == [] and len(h.attempts()) == 1
    assert len(h.events("job.attempt.failed")) == 2
    assert "encoder exploded" in h.events("job.attempt.failed")[1]["error_message"]


def test_concurrent_duplicate_is_skipped_while_lock_is_held(h):
    h.redis.set(f"vp:lock:transcode:{JOB_ID}:1", "another-worker", nx=True, ex=30)
    h.worker.handle(h.message())
    assert h.attempts() == [] and h.encoded == [] and h.events("job.attempt.started") == []


def test_lock_is_released_after_processing(h):
    h.worker.handle(h.message())
    assert h.redis.keys() == []


def test_each_attempt_number_is_processed_separately(h):
    h.fail_with = FFmpegError("transient")
    h.worker.handle(h.message(attempt=1))
    h.fail_with = None
    h.worker.handle(h.message(attempt=2))
    assert [(a.attempt_number, a.status) for a in h.attempts()] == [(1, "failed"), (2, "succeeded")]


def test_failed_attempt_stores_error_and_reports_retryable_failure(h):
    h.fail_with = FFmpegError("ffmpeg exited with code 1: Invalid data found")
    h.worker.handle(h.message())

    (attempt,) = h.attempts()
    assert attempt.status == "failed" and attempt.error_type == "FFmpegError"
    assert "Invalid data found" in attempt.error_message and attempt.duration_ms is not None
    assert h.outputs() == []
    (failed,) = h.events("job.attempt.failed")
    assert failed["retryable"] is True and failed["attempt"] == 1 and failed["job_id"] == JOB_ID
    assert h.events("job.attempt.succeeded") == []
    assert h.redis.keys() == []  # lock released even on failure


def test_partial_outputs_are_cleaned_up_when_a_later_resolution_fails(tmp_path):
    h = Harness(tmp_path)
    original = h.worker.transcode
    state = {"n": 0}

    def flaky(source, dest, height, **kw):
        state["n"] += 1
        if state["n"] == 2:
            raise FFmpegError("second rendition failed")
        return original(source, dest, height, **kw)

    h.worker.transcode = flaky
    h.worker.handle(h.message(resolutions=(360, 720)))
    assert h.attempts()[0].status == "failed"
    assert h.outputs() == []
    outputs_dir = h.storage.root / "outputs" / VIDEO_ID
    assert not outputs_dir.exists() or list(outputs_dir.iterdir()) == []  # no half-finished renditions served


def test_missing_source_is_a_non_retryable_failure(h):
    h.storage.resolve(f"uploads/{VIDEO_ID}/original.mp4").unlink()
    h.worker.handle(h.message())
    (failed,) = h.events("job.attempt.failed")
    assert failed["retryable"] is False and "not found" in failed["error_message"]


def test_unreadable_media_is_non_retryable(h):
    h.probe_error = FFmpegError("not a readable video file", retryable=False)
    h.worker.handle(h.message())
    assert h.events("job.attempt.failed")[0]["retryable"] is False


def test_simulated_failure_switch_fails_first_n_attempts(tmp_path):
    h = Harness(tmp_path, simulate_failure_attempts=1)
    h.worker.handle(h.message(attempt=1))
    h.worker.handle(h.message(attempt=2))
    assert [(a.attempt_number, a.status) for a in h.attempts()] == [(1, "failed"), (2, "succeeded")]


def test_abandoned_running_attempt_is_taken_over_when_lock_expired(h):
    """Worker crashed mid-job: the row says 'running' but no lock is held any more."""
    with h.db.session() as s:
        s.add(ProcessingAttempt(job_id=JOB_ID, video_id=VIDEO_ID, attempt_number=1, status="running", worker_id="dead-worker", started_at=utcnow()))
    h.worker.handle(h.message())  # broker redelivers the unacked message
    (attempt,) = h.attempts()
    assert attempt.status == "succeeded" and attempt.worker_id == h.worker.worker_id
    assert len(h.outputs()) == 2


def test_running_attempt_with_live_lock_is_left_alone(h):
    with h.db.session() as s:
        s.add(ProcessingAttempt(job_id=JOB_ID, video_id=VIDEO_ID, attempt_number=1, status="running", worker_id="alive", started_at=utcnow()))
    h.redis.set(f"vp:lock:transcode:{JOB_ID}:1", "alive", nx=True, ex=30)
    h.worker.handle(h.message())
    assert h.encoded == [] and h.attempts()[0].worker_id == "alive"


def test_database_guard_works_without_redis(h):
    h.redis.down = True
    h.worker.handle(h.message())
    h.encoded.clear()
    h.worker.handle(h.message())  # duplicate while redis is still down
    assert h.encoded == [] and len(h.attempts()) == 1


def test_without_redis_a_fresh_running_attempt_is_not_stolen_but_a_stale_one_is(h):
    h.redis.down = True
    with h.db.session() as s:
        s.add(ProcessingAttempt(job_id=JOB_ID, video_id=VIDEO_ID, attempt_number=1, status="running", worker_id="w", started_at=utcnow()))
    h.worker.handle(h.message())
    assert h.encoded == []  # could still be running somewhere

    with h.db.session() as s:
        s.execute(update(ProcessingAttempt).values(started_at=utcnow() - timedelta(seconds=10_000)))
    h.worker.handle(h.message())
    assert h.attempts()[0].status == "succeeded"


def test_rerun_replaces_previous_outputs_for_same_resolution(h):
    h.worker.handle(h.message(attempt=1, resolutions=(360,)))
    first = h.outputs()[0].id
    h.worker.handle(h.message(attempt=2, resolutions=(360, 720)))
    heights = [o.height for o in h.outputs()]
    assert heights == [360, 720]  # unique per (video, height)
    assert h.outputs()[0].id != first


@pytest.mark.parametrize("body", [{}, {"job_id": JOB_ID}, {"job_id": JOB_ID, "video_id": VIDEO_ID, "attempt": 0}, "nonsense"])
def test_malformed_message_is_rejected_to_the_dead_letter_queue(h, body):
    msg = Message("transcode", body if isinstance(body, dict) else {"x": body})
    with pytest.raises(RejectMessage):
        h.worker.handle(msg)
    assert h.attempts() == []


def test_malformed_message_with_known_job_marks_it_failed_instead_of_hanging(h):
    msg = Message("transcode", {"job_id": JOB_ID, "video_id": VIDEO_ID, "attempt": 1})  # no source/resolutions
    with pytest.raises(RejectMessage):
        h.worker.handle(msg)
    (failed,) = h.events("job.attempt.failed")
    assert failed["error_type"] == "InvalidMessage" and failed["retryable"] is False


def test_poison_message_lands_in_dlq_through_the_broker(h):
    h.broker.consume(__import__("vp_common.topology", fromlist=["Q_TRANSCODE"]).Q_TRANSCODE, h.worker.handle)
    h.broker.publish("vp.jobs", "transcode", {"garbage": True})
    assert len(h.broker.dead_letters["transcode.jobs"]) == 1


def test_outputs_and_attempts_api(h):
    from fastapi.testclient import TestClient

    h.worker.handle(h.message())
    client = TestClient(h.app)
    outputs = client.get(f"/internal/videos/{VIDEO_ID}/outputs").json()["outputs"]
    assert [o["resolution"] for o in outputs] == ["360p", "720p"]
    attempts = client.get(f"/internal/jobs/{JOB_ID}/attempts").json()
    assert attempts[0]["status"] == "succeeded"
    assert client.get("/internal/videos/not-a-uuid/outputs").status_code == 422
