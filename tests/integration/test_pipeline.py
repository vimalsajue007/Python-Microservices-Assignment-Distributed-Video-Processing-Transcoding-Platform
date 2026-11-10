"""End-to-end flow from upload to processed output, in-process.

All seven services run for real (own databases, real FFmpeg); only RabbitMQ and
Redis are replaced by in-memory equivalents with identical delivery semantics.
The same scenarios run against the real docker compose stack in tests/e2e.
"""

from __future__ import annotations

import subprocess

import pytest

from helpers.videos import make_video

pytestmark = pytest.mark.ffmpeg


@pytest.fixture(scope="module")
def hd_video(tmp_path_factory):
    return make_video(tmp_path_factory.mktemp("hd") / "hd.mp4", width=1280, height=720, seconds=1.5)


def _height(path) -> int:
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=height", "-of", "csv=p=0", str(path)],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    return int(out)


def test_upload_to_processed_output(platform, hd_video):
    # 1. upload through the gateway
    resp = platform.upload(hd_video, title="Launch teaser")
    assert resp.status_code == 201, resp.text
    video = resp.json()
    video_id = video["id"]
    assert video["title"] == "Launch teaser" and video["size_bytes"] == hd_video.stat().st_size

    # 2. async consumers filled metadata (video.uploaded event) and thumbnails
    meta = platform.video(video_id)
    assert meta["status"] == "uploaded"
    assert (meta["width"], meta["height"]) == (1280, 720)
    assert meta["duration_seconds"] == pytest.approx(1.5, abs=0.3)
    assert meta["format"] == "mp4" and meta["video_codec"] == "h264"

    thumbs = platform.gateway.get(f"/videos/{video_id}/thumbnails").json()
    assert thumbs["status"] == "completed" and thumbs["count"] == 3
    assert all(t["url"] and t["status"] == "completed" for t in thumbs["thumbnails"])

    # 3. trigger processing: returns immediately with a queued/advancing job
    resp = platform.process(video_id, resolutions=[360, 720, 1080])
    assert resp.status_code == 202, resp.text
    job_id = resp.json()["id"]

    # 4. job completed; 1080p skipped because the source is only 720p (no upscaling)
    job = platform.job(job_id)
    assert job["status"] == "completed" and job["progress"] == 100
    assert job["resolutions"] == [360, 720]
    assert [h["status"] for h in job["attempt_history"]] == ["succeeded"]

    status = platform.gateway.get(f"/videos/{video_id}/status").json()
    assert status["status"] == "completed" and status["progress"] == 100 and status["job_id"] == job_id

    # 5. outputs with signed URLs that actually stream
    outputs = platform.gateway.get(f"/videos/{video_id}/outputs").json()
    assert outputs["count"] == 2
    assert [o["resolution"] for o in outputs["outputs"]] == ["360p", "720p"]
    low = outputs["outputs"][0]
    media = platform.gateway.get(low["url"])
    assert media.status_code == 200 and media.headers["content-type"] == "video/mp4"
    assert len(media.content) == low["size_bytes"]
    produced = platform.storage / "outputs" / video_id / "360p.mp4"
    assert _height(produced) == 360

    # 6. HTTP range requests (needed for seeking in the <video> element)
    part = platform.gateway.get(low["url"], headers={"Range": "bytes=0-99"})
    assert part.status_code == 206 and len(part.content) == 100
    assert part.headers["content-range"].startswith("bytes 0-99/")

    # 7. thumbnails are images
    thumb = platform.gateway.get(thumbs["thumbnails"][0]["url"])
    assert thumb.status_code == 200 and thumb.headers["content-type"] == "image/jpeg"
    assert thumb.content[:2] == b"\xff\xd8"

    # 8. listing reflects the final state
    listing = platform.gateway.get("/videos").json()
    assert listing["total"] == 1 and listing["items"][0]["status"] == "completed"


def test_small_source_is_not_upscaled(platform, small_video):
    video_id = platform.upload(small_video).json()["id"]
    job = platform.process(video_id).json()
    final = platform.job(job["id"])
    assert final["status"] == "completed"
    assert final["resolutions"] == [180]  # native height kept; 360/720/1080 would upscale
    outputs = platform.gateway.get(f"/videos/{video_id}/outputs").json()
    assert [o["resolution"] for o in outputs["outputs"]] == ["180p"]


def test_processing_does_not_block_the_api(make_platform, sample_video):
    """With the consumers busy/unavailable the API still answers instantly and the job waits."""
    p = make_platform()
    p.broker.auto_pump = False
    video_id = p.upload(sample_video).json()["id"]
    p.broker.pump()  # let metadata/thumbnail process the upload event

    resp = p.process(video_id)
    assert resp.status_code == 202
    assert resp.json()["status"] == "queued"
    assert p.broker.pending("transcode.jobs") == 1

    p.broker.pump()  # now the worker consumes it
    assert p.job(resp.json()["id"])["status"] == "completed"


def test_worker_failures_are_retried_until_success(make_platform, sample_video):
    p = make_platform(transcoder={"simulate_failure_attempts": 2})
    video_id = p.upload(sample_video).json()["id"]
    job_id = p.process(video_id).json()["id"]

    job = p.job(job_id)
    assert job["status"] == "retrying" and job["attempt"] == 1
    assert "Simulated failure" in job["last_error"]
    assert p.video(video_id)["status"] == "processing"  # still in flight for the UI

    p.run_scheduler()  # backoff elapsed -> attempt 2 (fails) -> attempt 3 (succeeds)

    job = p.job(job_id)
    assert job["status"] == "completed" and job["attempt"] == 3
    assert [h["status"] for h in job["attempt_history"]] == ["failed", "failed", "succeeded"]
    assert job["dead_lettered"] is False
    assert p.video(video_id)["status"] == "completed"

    attempts = p.direct["transcoder"].get(f"/internal/jobs/{job_id}/attempts").json()
    assert [(a["attempt_number"], a["status"]) for a in attempts] == [(1, "failed"), (2, "failed"), (3, "succeeded")]
    assert attempts[0]["error_type"] == "TranscodeError" and attempts[0]["error_message"]


def test_retry_limit_dead_letters_job_and_manual_retry_recovers(make_platform, sample_video):
    p = make_platform(transcoder={"simulate_failure_attempts": 99})
    video_id = p.upload(sample_video).json()["id"]
    job_id = p.process(video_id).json()["id"]
    p.run_scheduler()

    job = p.job(job_id)
    assert job["status"] == "failed" and job["attempt"] == 3 and job["max_attempts"] == 3
    assert job["dead_lettered"] is True and job["retries_remaining"] == 0
    assert [h["status"] for h in job["attempt_history"]] == ["failed"] * 3
    video = p.video(video_id)
    assert video["status"] == "failed" and "Simulated failure" in video["error_message"]

    # the exhausted job landed in the dead-letter queue for inspection
    dead = p.broker.dead_letters["transcode.jobs"]
    assert len(dead) == 1 and dead[0].body["job_id"] == job_id and dead[0].body["reason"] == "attempts exhausted"

    # operator fixes the root cause and retries via the API
    p.apps["transcoder"].state.settings.simulate_failure_attempts = 0
    retry = p.gateway.post(f"/jobs/{job_id}/retry")
    assert retry.status_code == 202, retry.text
    job = p.job(job_id)
    assert job["status"] == "completed" and job["attempt"] == 4 and job["max_attempts"] == 6
    assert p.video(video_id)["status"] == "completed"


def test_duplicate_job_message_is_processed_once(platform, sample_video):
    video_id = platform.upload(sample_video).json()["id"]
    job_id = platform.process(video_id).json()["id"]
    assert platform.job(job_id)["status"] == "completed"

    from vp_common.messaging import Message

    original = next(b for ex, rk, b in platform.broker.published if rk == "transcode")
    calls: list[str] = []
    worker = platform.worker
    real_transcode = worker.transcode
    worker.transcode = lambda *a, **k: (calls.append("ffmpeg"), real_transcode(*a, **k))[1]

    for _ in range(3):  # broker redelivers the very same message (at-least-once)
        platform.broker.redeliver("transcode.jobs", Message("transcode", dict(original)))

    assert calls == []  # FFmpeg never ran again
    attempts = platform.direct["transcoder"].get(f"/internal/jobs/{job_id}/attempts").json()
    assert len(attempts) == 1
    outputs = platform.direct["transcoder"].get(f"/internal/videos/{video_id}/outputs").json()["outputs"]
    assert [o["height"] for o in outputs] == [360]  # the 360p source yields exactly one rendition, never duplicated
    job = platform.job(job_id)
    assert job["status"] == "completed" and len(job["attempt_history"]) == 1


def test_one_active_job_per_video(make_platform, sample_video):
    p = make_platform()
    p.broker.auto_pump = False
    video_id = p.upload(sample_video).json()["id"]
    p.broker.pump()
    first = p.process(video_id)
    second = p.process(video_id)
    assert first.status_code == 202
    assert second.status_code == 409
    assert second.json()["error"]["code"] == "job_in_progress"
    assert second.json()["error"]["details"]["job_id"] == first.json()["id"]


def test_process_unknown_video_is_404(platform):
    resp = platform.process("11111111-1111-4111-8111-111111111111")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"
    assert resp.json()["request_id"]


def test_undecodable_video_is_flagged_and_cannot_be_processed(platform):
    from helpers.videos import fake_mp4_bytes

    resp = platform.upload(fake_mp4_bytes(), filename="broken.mp4")
    assert resp.status_code == 201  # passes the cheap signature check
    video_id = resp.json()["id"]

    meta = platform.video(video_id)
    assert meta["status"] == "invalid" and meta["error_message"]
    assert platform.gateway.get(f"/videos/{video_id}/thumbnails").json()["status"] == "failed"

    proc = platform.process(video_id)
    assert proc.status_code == 422 and proc.json()["error"]["code"] == "video_not_processable"


def test_stale_worker_is_detected_and_job_retried(make_platform, sample_video):
    """A worker that dies mid-job stops sending heartbeats: the scheduler fails the attempt and retries."""
    from datetime import timedelta

    from sqlalchemy import update

    from orchestrator_service.models import ProcessingJob
    from vp_common.db import utcnow
    from vp_common.messaging import Message

    p = make_platform(orchestrator={"job_stale_seconds": 60})
    p.broker.auto_pump = False
    video_id = p.upload(sample_video).json()["id"]
    p.broker.pump()
    job_id = p.process(video_id).json()["id"]

    # simulate: worker announced the start, then died (no progress/result events ever arrive)
    from vp_common.events import AttemptStarted

    p.orchestrator.handle(
        Message("job.attempt.started", AttemptStarted(job_id=job_id, video_id=video_id, attempt=1, worker_id="dead-worker").to_body())
    )
    assert p.job(job_id)["status"] == "running"
    p.broker._queues["transcode.jobs"].clear()  # the lost delivery

    db = p.apps["orchestrator"].state.db
    with db.session() as s:
        s.execute(update(ProcessingJob).where(ProcessingJob.id == job_id).values(heartbeat_at=utcnow() - timedelta(seconds=600)))

    stats = p.orchestrator.tick()
    assert stats["stale"] == 1
    job = p.job(job_id)
    assert job["status"] == "retrying" and "WorkerTimeout" in job["last_error"]

    p.broker.auto_pump = True
    p.run_scheduler()
    assert p.job(job_id)["status"] == "completed"
