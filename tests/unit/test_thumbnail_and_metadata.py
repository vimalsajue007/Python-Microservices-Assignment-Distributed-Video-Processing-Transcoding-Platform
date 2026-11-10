"""Unit tests: thumbnail generation and metadata service behaviour."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from helpers.fakes import FakeRedis
from helpers.videos import fake_mp4_bytes
from metadata_service.models import VideoMetadata
from thumbnail_service.config import Settings as ThumbSettings
from thumbnail_service.main import create_app as create_thumbnail
from thumbnail_service.models import VideoThumbnail
from vp_common.events import JobStatusChanged, VideoUploaded
from vp_common.ffmpeg import FFmpegError, ProbeResult
from vp_common.db import utcnow
from vp_common.messaging import InMemoryBroker, Message, RejectMessage

VIDEO_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def uploaded_event(**over) -> Message:
    ev = VideoUploaded(
        video_id=VIDEO_ID, title="T", original_filename="t.mp4", format="mp4", content_type="video/mp4",
        size_bytes=10, storage_path=f"uploads/{VIDEO_ID}/original.mp4", checksum_sha256="0" * 64, uploaded_at=utcnow(), **over,
    )
    return Message("video.uploaded", ev.to_body())


# --------------------------------------------------------------------------- #
# thumbnails (fake ffmpeg)
# --------------------------------------------------------------------------- #
class ThumbHarness:
    def __init__(self, tmp_path, **settings):
        self.extracted: list[float] = []
        self.fail_times = 0
        self.probe_error: Exception | None = None
        self.broker = InMemoryBroker()
        self.redis = FakeRedis()

        def probe(path, **kw):
            if self.probe_error:
                raise self.probe_error
            return ProbeResult(duration=8.0, width=1280, height=720, container="mp4", video_codec="h264", audio_codec=None, bit_rate=None, frame_rate=25, size_bytes=1)

        def extract(source, dest, at, **kw):
            if self.fail_times > 0:
                self.fail_times -= 1
                raise FFmpegError("seek failed")
            self.extracted.append(round(at, 2))
            Path(dest).write_bytes(b"\xff\xd8jpeg")

        values = dict(database_url=f"sqlite:///{tmp_path / 'th.db'}", auto_create_schema=True, enable_background=False,
                      storage_root=str(tmp_path / "media"))
        values.update(settings)
        self.app = create_thumbnail(ThumbSettings(**values), broker=self.broker, redis_client=self.redis, probe_fn=probe, extract_fn=extract)
        self.worker = self.app.state.worker
        self.db = self.app.state.db
        self.worker.storage.prepare(f"uploads/{VIDEO_ID}/original.mp4").write_bytes(b"src")

    def rows(self):
        with self.db.session() as s:
            return list(s.scalars(select(VideoThumbnail).order_by(VideoThumbnail.thumbnail_index)))


@pytest.fixture
def th(tmp_path):
    return ThumbHarness(tmp_path)


def test_generates_evenly_spaced_thumbnails_and_stores_references(th):
    th.worker.handle(uploaded_event())
    rows = th.rows()
    assert [r.status for r in rows] == ["completed"] * 3
    assert th.extracted == [2.0, 4.0, 6.0]  # 8s video -> 25%, 50%, 75%
    assert [r.relative_path for r in rows] == [f"thumbnails/{VIDEO_ID}/thumb_{i}.jpg" for i in range(3)]
    assert all((th.worker.storage.root / r.relative_path).exists() for r in rows)
    assert [(r.width, r.height) for r in rows] == [(1280, 720)] * 3
    assert len(th.broker.messages("thumbnail.generated")) == 3


def test_duplicate_video_uploaded_event_is_idempotent(th):
    th.worker.handle(uploaded_event())
    th.extracted.clear()
    th.worker.handle(uploaded_event())
    assert th.extracted == [] and len(th.rows()) == 3


def test_concurrent_generation_for_same_video_is_skipped(th):
    th.redis.set(f"vp:lock:thumbnails:{VIDEO_ID}", "other", nx=True, ex=60)
    th.worker.handle(uploaded_event())
    assert th.rows() == [] and th.extracted == []


def test_ffmpeg_failure_is_retried_then_recorded(tmp_path):
    th = ThumbHarness(tmp_path, ffmpeg_attempts=2)
    th.fail_times = 2  # thumbnail 0 fails both attempts, the others succeed
    th.worker.handle(uploaded_event())
    rows = th.rows()
    assert [r.status for r in rows] == ["failed", "completed", "completed"]
    assert rows[0].error_message == "seek failed" and rows[0].attempts == 2 and rows[0].relative_path is None


def test_transient_failure_succeeds_on_second_try_using_first_frame(tmp_path):
    th = ThumbHarness(tmp_path, ffmpeg_attempts=2)
    th.fail_times = 1
    th.worker.handle(uploaded_event())
    assert [r.status for r in th.rows()] == ["completed"] * 3
    assert th.extracted[0] == 0.0  # fell back to frame 0


def test_failed_thumbnails_are_retried_when_the_event_is_redelivered(tmp_path):
    th = ThumbHarness(tmp_path, ffmpeg_attempts=1)
    th.fail_times = 1
    th.worker.handle(uploaded_event())
    assert [r.status for r in th.rows()] == ["failed", "completed", "completed"]
    th.worker.handle(uploaded_event())
    assert [r.status for r in th.rows()] == ["completed"] * 3


def test_unreadable_video_marks_every_thumbnail_failed(th):
    th.probe_error = FFmpegError("not a readable video file", retryable=False)
    th.worker.handle(uploaded_event())
    rows = th.rows()
    assert [r.status for r in rows] == ["failed"] * 3 and "Unreadable video" in rows[0].error_message


def test_transient_probe_error_is_raised_for_redelivery(th):
    th.probe_error = FFmpegError("ffprobe timed out", retryable=True)
    with pytest.raises(FFmpegError):
        th.worker.handle(uploaded_event())


def test_invalid_event_is_rejected(th):
    with pytest.raises(RejectMessage):
        th.worker.handle(Message("video.uploaded", {"video_id": VIDEO_ID}))


def test_thumbnail_count_is_configurable(tmp_path):
    th = ThumbHarness(tmp_path, thumbnail_count=5)
    th.worker.handle(uploaded_event())
    assert len(th.rows()) == 5


def test_thumbnail_api_reports_aggregate_status(th):
    client = TestClient(th.app)
    assert client.get(f"/internal/videos/{VIDEO_ID}/thumbnails").json() == {"video_id": VIDEO_ID, "status": "none", "items": []}
    th.worker.handle(uploaded_event())
    body = client.get(f"/internal/videos/{VIDEO_ID}/thumbnails").json()
    assert body["status"] == "completed" and [i["thumbnail_index"] for i in body["items"]] == [0, 1, 2]


# --------------------------------------------------------------------------- #
# metadata service (real ffprobe through the platform harness)
# --------------------------------------------------------------------------- #
@pytest.mark.ffmpeg
def test_metadata_is_extracted_from_the_uploaded_file(platform, sample_video):
    video_id = platform.upload(sample_video, filename="sample.mp4", title="Sample").json()["id"]
    meta = platform.gateway.get(f"/videos/{video_id}").json()
    assert meta["title"] == "Sample" and meta["original_filename"] == "sample.mp4"
    assert (meta["width"], meta["height"]) == (640, 360)
    assert meta["duration_seconds"] == pytest.approx(2.0, abs=0.3)
    assert meta["audio_codec"] == "aac" and meta["frame_rate"] == pytest.approx(15, abs=0.1)
    assert meta["size_bytes"] == sample_video.stat().st_size
    assert meta["status"] == "uploaded" and meta["progress"] == 0


def test_metadata_for_undecodable_file_is_flagged_invalid(platform):
    video_id = platform.upload(fake_mp4_bytes(), filename="x.mp4").json()["id"]
    meta = platform.gateway.get(f"/videos/{video_id}").json()
    assert meta["status"] == "invalid" and "not a readable video" in meta["error_message"]


def test_duplicate_video_uploaded_event_does_not_duplicate_metadata(platform):
    video_id = platform.upload(fake_mp4_bytes()).json()["id"]
    original = next(b for _, rk, b in platform.broker.published if rk == "video.uploaded")
    platform.broker.redeliver("metadata.events", Message("video.uploaded", dict(original)))
    assert platform.gateway.get("/videos").json()["total"] == 1


def test_out_of_order_status_events_are_ignored(platform):
    video_id = platform.upload(fake_mp4_bytes()).json()["id"]
    svc = platform.apps["metadata"].state.service
    db = platform.apps["metadata"].state.db
    with db.session() as s:  # pretend the video is analysable
        row = s.get(VideoMetadata, video_id)
        row.status, row.analysis_status, row.error_message = "uploaded", "ok", None

    def event(status, progress, at):
        return Message("job.status_changed", JobStatusChanged(job_id="j" * 36, video_id=video_id, status=status, progress=progress, occurred_at=at).to_body())

    from datetime import timedelta

    t = utcnow()
    svc.handle(event("running", 50, t + timedelta(seconds=2)))
    svc.handle(event("queued", 0, t + timedelta(seconds=1)))  # arrives late
    status = platform.gateway.get(f"/videos/{video_id}/status").json()
    assert status["status"] == "processing" and status["progress"] == 50

    svc.handle(event("completed", 100, t + timedelta(seconds=3)))
    assert platform.gateway.get(f"/videos/{video_id}/status").json() == {**status, "status": "completed", "progress": 100, "updated_at": platform.gateway.get(f"/videos/{video_id}/status").json()["updated_at"]}


def test_status_event_for_unknown_video_is_ignored_and_bad_status_rejected(platform):
    svc = platform.apps["metadata"].state.service
    svc.handle(Message("job.status_changed", JobStatusChanged(job_id="j" * 36, video_id=VIDEO_ID, status="running").to_body()))
    with pytest.raises(RejectMessage):
        svc.handle(Message("job.status_changed", JobStatusChanged(job_id="j" * 36, video_id=VIDEO_ID, status="exploded").to_body()))
    with pytest.raises(RejectMessage):
        svc.handle(Message("job.status_changed", {"nonsense": 1}))


def test_list_filters_search_and_pagination(platform):
    ids = []
    for i, name in enumerate(["alpha", "beta", "gamma"]):
        ids.append(platform.upload(fake_mp4_bytes(), filename=f"{name}.mp4", title=f"Clip {name}").json()["id"])
    db = platform.apps["metadata"].state.db
    with db.session() as s:
        s.get(VideoMetadata, ids[0]).status = "completed"

    listing = platform.gateway.get("/videos").json()
    assert listing["total"] == 3 and [i["title"] for i in listing["items"]] == ["Clip gamma", "Clip beta", "Clip alpha"]  # newest first
    assert platform.gateway.get("/videos", params={"status": "completed"}).json()["total"] == 1
    assert [i["title"] for i in platform.gateway.get("/videos", params={"q": "beta"}).json()["items"]] == ["Clip beta"]
    page = platform.gateway.get("/videos", params={"limit": 2, "offset": 2}).json()
    assert page["total"] == 3 and len(page["items"]) == 1 and page["limit"] == 2 and page["offset"] == 2


def test_unknown_video_endpoints_return_404_envelope(platform):
    for path in (f"/videos/{VIDEO_ID}", f"/videos/{VIDEO_ID}/status"):
        resp = platform.gateway.get(path)
        assert resp.status_code == 404 and resp.json()["error"]["code"] == "video_not_found"


def test_internal_endpoint_exposes_storage_path_but_public_one_does_not(platform):
    video_id = platform.upload(fake_mp4_bytes()).json()["id"]
    internal = platform.direct["metadata"].get(f"/internal/videos/{video_id}").json()
    assert internal["storage_path"] == f"uploads/{video_id}/original.mp4" and "analysis_status" in internal
    assert "storage_path" not in platform.gateway.get(f"/videos/{video_id}").json()
    assert platform.gateway.get(f"/internal/videos/{video_id}").status_code == 404  # not routed by the gateway
