"""End-to-end tests against a RUNNING stack (docker compose or scripts/dev_stack.sh).

    docker compose up -d --build
    E2E_BASE_URL=http://localhost:8000 pytest tests/e2e -v

Everything goes through the public API gateway; nothing is mocked: real
RabbitMQ, Redis, MySQL, FFmpeg and all seven services.

Optional environment:
    E2E_BASE_URL            gateway URL (tests are skipped when unset)
    E2E_TIMEOUT             seconds to wait for processing (default 180)
    E2E_MAX_UPLOAD_MB       configured MAX_UPLOAD_BYTES in MiB -> enables the oversize test
    E2E_EXPECT_FAILURES     "1" when the stack runs with SIMULATE_FAILURE_ATTEMPTS=2 -> enables the retry test
"""

from __future__ import annotations

import io
import os
import time

import httpx
import pytest

from sample_media import SAMPLE_MP4

BASE_URL = os.environ.get("E2E_BASE_URL", "").rstrip("/")
TIMEOUT = float(os.environ.get("E2E_TIMEOUT", "180"))

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(not BASE_URL, reason="set E2E_BASE_URL to run against a live stack"),
]


@pytest.fixture(scope="module")
def api():
    with httpx.Client(base_url=BASE_URL, timeout=60) as client:
        yield client


def wait_for(predicate, *, timeout: float = TIMEOUT, interval: float = 1.0, what: str = "condition"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    pytest.fail(f"timed out waiting for {what} (last value: {last!r})")


def upload(api, data=SAMPLE_MP4, filename="e2e-sample.mp4", content_type="video/mp4", title="E2E sample"):
    return api.post("/videos/upload", files={"file": (filename, io.BytesIO(data), content_type)}, data={"title": title})


def await_job(api, job_id, *, until=("completed", "failed")):
    return wait_for(
        lambda: (j if (j := api.get(f"/jobs/{job_id}").json())["status"] in until else None),
        what=f"job {job_id} to reach {until}",
    )


def uploaded_and_analysed(api) -> str:
    resp = upload(api)
    assert resp.status_code == 201, resp.text
    video_id = resp.json()["id"]
    wait_for(
        lambda: (m := api.get(f"/videos/{video_id}")).status_code == 200 and m.json()["width"] == 1280,
        what="metadata extraction",
    )
    return video_id


# --------------------------------------------------------------------------- #
def test_gateway_and_all_services_are_healthy(api):
    health = wait_for(lambda: (h := api.get("/health/services").json())["status"] == "ok" and h, what="all services healthy")
    assert set(health["services"]) == {"upload", "metadata", "orchestrator", "transcoder", "thumbnail", "media"}


def test_upload_to_processed_output(api):
    # upload
    video_id = uploaded_and_analysed(api)
    meta = api.get(f"/videos/{video_id}").json()
    assert meta["title"] == "E2E sample" and meta["status"] == "uploaded"
    assert (meta["width"], meta["height"]) == (1280, 720) and meta["video_codec"] == "h264"

    # thumbnails are generated asynchronously right after the upload
    thumbs = wait_for(lambda: (t := api.get(f"/videos/{video_id}/thumbnails").json())["status"] == "completed" and t, what="thumbnails")
    assert thumbs["count"] == 3
    image = api.get(thumbs["thumbnails"][0]["url"])
    assert image.status_code == 200 and image.headers["content-type"] == "image/jpeg" and image.content[:2] == b"\xff\xd8"

    # processing is asynchronous: the API answers immediately
    started = time.monotonic()
    resp = api.post(f"/videos/{video_id}/process", json={"resolutions": [360, 720, 1080]})
    assert resp.status_code == 202, resp.text
    assert time.monotonic() - started < 5
    job = resp.json()
    assert job["status"] in ("queued", "running")

    # a second request while the job is active is refused
    assert api.post(f"/videos/{video_id}/process").status_code == 409

    # track progress until completion
    final = await_job(api, job["id"])
    assert final["status"] == "completed", final
    assert final["progress"] == 100 and final["resolutions"] == [360, 720]  # 1080p skipped: source is 720p

    status = api.get(f"/videos/{video_id}/status").json()
    assert status["status"] == "completed" and status["job_id"] == job["id"]

    # outputs + secure playback
    outputs = api.get(f"/videos/{video_id}/outputs").json()
    assert [o["resolution"] for o in outputs["outputs"]] == ["360p", "720p"]
    for out in outputs["outputs"]:
        media = api.get(out["url"])
        assert media.status_code == 200 and len(media.content) == out["size_bytes"] > 0
    partial = api.get(outputs["outputs"][0]["url"], headers={"Range": "bytes=0-1023"})
    assert partial.status_code == 206 and len(partial.content) == 1024

    # tampering with the signature is refused
    forged = outputs["outputs"][0]["url"].replace("sig=", "sig=0")
    assert api.get(forged).status_code == 403

    listing = api.get("/videos", params={"status": "completed"}).json()
    assert any(v["video_id"] == video_id for v in listing["items"])


def test_invalid_formats_are_rejected(api):
    assert upload(api, b"just some text", "notes.txt", "text/plain").status_code == 415
    assert upload(api, b"MZ" + b"\x00" * 500, "fake.mp4", "video/mp4").status_code == 415  # not an MP4 signature
    resp = upload(api, b"", "empty.mp4", "video/mp4")
    assert resp.status_code in (400, 422)
    err = upload(api, b"just some text", "notes.txt", "text/plain").json()
    assert err["error"]["code"] == "unsupported_media_type" and err["request_id"]


@pytest.mark.skipif(not os.environ.get("E2E_MAX_UPLOAD_MB"), reason="set E2E_MAX_UPLOAD_MB to the configured limit")
def test_oversized_upload_is_rejected(api):
    limit = int(os.environ["E2E_MAX_UPLOAD_MB"]) * 1024 * 1024
    payload = SAMPLE_MP4 + b"\x00" * (limit + 1 - len(SAMPLE_MP4))
    resp = upload(api, payload)
    assert resp.status_code == 413 and resp.json()["error"]["code"] == "payload_too_large"


def test_request_validation_and_not_found(api):
    assert api.get("/videos/not-a-uuid").status_code == 422
    assert api.get("/videos", params={"limit": 1000}).status_code == 422
    missing = api.get("/videos/11111111-1111-4111-8111-111111111111")
    assert missing.status_code == 404 and missing.json()["error"]["code"] == "video_not_found"
    assert api.post("/videos/11111111-1111-4111-8111-111111111111/process").status_code == 404
    assert api.get("/jobs/11111111-1111-4111-8111-111111111111").status_code == 404
    assert api.post("/videos/11111111-1111-4111-8111-111111111111/process", json={"resolutions": [999]}).status_code == 422


@pytest.mark.skipif(os.environ.get("E2E_EXPECT_FAILURES") != "1", reason="start the stack with SIMULATE_FAILURE_ATTEMPTS=2")
def test_worker_failures_are_retried(api):
    video_id = uploaded_and_analysed(api)
    job_id = api.post(f"/videos/{video_id}/process").json()["id"]
    final = await_job(api, job_id, until=("completed",))
    assert [h["status"] for h in final["attempt_history"]] == ["failed", "failed", "succeeded"]
    assert "Simulated failure" in final["attempt_history"][0]["error"]
