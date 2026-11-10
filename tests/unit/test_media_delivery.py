"""Unit tests: media delivery (secure access, signed URLs, range requests)."""

from __future__ import annotations

import time

import pytest

from vp_common.security import sign

VIDEO_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
SECRET = "test-secret"
DATA = bytes(range(256)) * 40  # 10 240 bytes


@pytest.fixture
def media(platform):
    out = platform.storage / "outputs" / VIDEO_ID
    out.mkdir(parents=True)
    (out / "360p.mp4").write_bytes(DATA)
    thumbs = platform.storage / "thumbnails" / VIDEO_ID
    thumbs.mkdir(parents=True)
    (thumbs / "thumb_0.jpg").write_bytes(b"\xff\xd8\xff" + b"0" * 100)
    return platform


def url(kind="outputs", name="360p.mp4", *, expires=None, secret=SECRET, video_id=VIDEO_ID):
    expires = expires or int(time.time()) + 600
    path = f"/media/{kind}/{video_id}/{name}"
    return f"{path}?expires={expires}&sig={sign(secret, path, expires)}"


def test_valid_signed_url_streams_the_file(media):
    resp = media.direct["media"].get(url())
    assert resp.status_code == 200 and resp.content == DATA
    assert resp.headers["content-type"] == "video/mp4"
    assert resp.headers["accept-ranges"] == "bytes"
    assert "private" in resp.headers["cache-control"]


def test_thumbnail_is_served_as_jpeg(media):
    resp = media.direct["media"].get(url("thumbnails", "thumb_0.jpg"))
    assert resp.status_code == 200 and resp.headers["content-type"] == "image/jpeg"


def test_range_requests_are_supported(media):
    client = media.direct["media"]
    part = client.get(url(), headers={"Range": "bytes=100-199"})
    assert part.status_code == 206 and part.content == DATA[100:200]
    assert part.headers["content-range"] == f"bytes 100-199/{len(DATA)}"
    tail = client.get(url(), headers={"Range": "bytes=10000-"})
    assert tail.status_code == 206 and tail.content == DATA[10000:]


def test_missing_or_garbage_signature_is_rejected(media):
    client = media.direct["media"]
    assert client.get(f"/media/outputs/{VIDEO_ID}/360p.mp4").status_code == 422  # params required
    bad = client.get(f"/media/outputs/{VIDEO_ID}/360p.mp4?expires={int(time.time()) + 600}&sig=deadbeef")
    assert bad.status_code == 403 and bad.json()["error"]["code"] == "invalid_signature"


def test_signature_made_with_another_secret_is_rejected(media):
    assert media.direct["media"].get(url(secret="attacker")).status_code == 403


def test_expired_link_is_rejected(media):
    assert media.direct["media"].get(url(expires=int(time.time()) - 10)).status_code == 403


def test_signature_does_not_transfer_to_another_file_or_video(media):
    client = media.direct["media"]
    good = url()
    swapped = good.replace("360p.mp4", "720p.mp4")
    assert client.get(swapped).status_code == 403
    other_video = good.replace(VIDEO_ID, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    assert client.get(other_video).status_code == 403


def test_unknown_kind_and_missing_file_are_404(media):
    client = media.direct["media"]
    assert client.get(url("secrets", "360p.mp4")).status_code == 404
    assert client.get(url(name="1080p.mp4")).status_code == 404  # valid signature, no such file


@pytest.mark.parametrize("name", ["..%2f..%2fetc%2fpasswd", "..", ".hidden", "a/b.mp4", "x" * 200 + ".mp4", "name with space.mp4"])
def test_path_traversal_and_odd_names_are_rejected(media, name):
    resp = media.direct["media"].get(f"/media/outputs/{VIDEO_ID}/{name}?expires={int(time.time()) + 60}&sig=" + "a" * 64)
    assert resp.status_code in (403, 404, 422)
    assert resp.status_code != 200


def test_invalid_video_id_is_422(media):
    assert media.direct["media"].get(url(video_id="../../etc")).status_code in (404, 422)


def test_outputs_endpoint_returns_resolutions_with_signed_urls(media, monkeypatch):
    # seed the transcoder's own database through its worker contract
    from transcoder_service.models import TranscodingOutput

    db = media.apps["transcoder"].state.db
    with db.session() as s:
        s.add(TranscodingOutput(video_id=VIDEO_ID, job_id="j" * 36, resolution="360p", height=360, width=640,
                                size_bytes=len(DATA), relative_path=f"outputs/{VIDEO_ID}/360p.mp4"))
    resp = media.direct["media"].get(f"/videos/{VIDEO_ID}/outputs")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    out = body["outputs"][0]
    assert (out["resolution"], out["height"], out["width"], out["content_type"]) == ("360p", 360, 640, "video/mp4")
    assert out["url"].startswith(f"/media/outputs/{VIDEO_ID}/360p.mp4?expires=") and "sig=" in out["url"]
    assert media.direct["media"].get(out["url"]).content == DATA  # the URL it hands out really works


def test_outputs_for_video_without_renditions_is_an_empty_list(media):
    body = media.direct["media"].get("/videos/cccccccc-cccc-4ccc-8ccc-cccccccccccc/outputs").json()
    assert body["count"] == 0 and body["outputs"] == []


def test_thumbnails_endpoint_only_signs_completed_thumbnails(media):
    from thumbnail_service.models import VideoThumbnail

    db = media.apps["thumbnail"].state.db
    with db.session() as s:
        s.add(VideoThumbnail(video_id=VIDEO_ID, thumbnail_index=0, status="completed", relative_path=f"thumbnails/{VIDEO_ID}/thumb_0.jpg", timestamp_seconds=1.0))
        s.add(VideoThumbnail(video_id=VIDEO_ID, thumbnail_index=1, status="failed", error_message="boom"))
    body = media.direct["media"].get(f"/videos/{VIDEO_ID}/thumbnails").json()
    assert body["status"] == "partial" and body["count"] == 2
    assert body["thumbnails"][0]["url"] and body["thumbnails"][1]["url"] is None
    assert body["thumbnails"][1]["error_message"] == "boom"


def test_upstream_unavailable_is_reported_as_503(make_platform):
    from fastapi.testclient import TestClient
    import httpx

    from media_service.config import Settings
    from media_service.main import create_app

    dead = httpx.Client(transport=httpx.MockTransport(lambda r: (_ for _ in ()).throw(httpx.ConnectError("refused"))), base_url="http://x")
    app = create_app(Settings(storage_root="/tmp"), transcoder_client=dead, thumbnail_client=dead)
    resp = TestClient(app).get(f"/videos/{VIDEO_ID}/outputs")
    assert resp.status_code == 503 and resp.json()["error"]["code"] == "upstream_unavailable"
