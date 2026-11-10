"""Unit tests: upload validation (type, signature, size) and upload service behaviour."""

from __future__ import annotations

import io

import pytest

from helpers.videos import fake_mp4_bytes
from upload_service import validation as v


# --------------------------------------------------------------------------- #
# pure validation helpers
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("name,ext", [("clip.mp4", ".mp4"), ("CLIP.MOV", ".mov"), ("a b.mkv", ".mkv"), ("x.webm", ".webm"), ("x.avi", ".avi"), ("x.m4v", ".m4v")])
def test_allowed_extensions(name, ext):
    assert v.validate_filename(name) == (name, ext)


@pytest.mark.parametrize("name", ["notes.txt", "malware.exe", "archive.zip", "image.png", "noextension", "video.mp4.exe", ".mp4x"])
def test_disallowed_extensions_are_rejected_with_415(name):
    with pytest.raises(v.UploadValidationError) as exc:
        v.validate_filename(name)
    assert exc.value.status_code == 415 and exc.value.code == "unsupported_media_type"


def test_missing_filename_is_422():
    with pytest.raises(v.UploadValidationError) as exc:
        v.validate_filename("")
    assert exc.value.status_code == 422


def test_filename_is_stripped_of_paths_and_control_characters():
    assert v.clean_filename("../../etc/passwd\x00.mp4") == "passwd.mp4"
    assert v.clean_filename("C:\\Users\\me\\movie.mp4") == "movie.mp4"


def test_content_type_must_match_extension():
    assert v.validate_content_type(".mp4", "video/mp4") == "video/mp4"
    assert v.validate_content_type(".mp4", "application/octet-stream") == "video/mp4"  # generic is tolerated
    assert v.validate_content_type(".mp4", None) == "video/mp4"
    with pytest.raises(v.UploadValidationError) as exc:
        v.validate_content_type(".mp4", "text/html")
    assert exc.value.status_code == 415


def test_signature_sniffing():
    assert v.sniff_matches(".mp4", fake_mp4_bytes()[:64])
    assert v.sniff_matches(".mkv", b"\x1a\x45\xdf\xa3" + b"\0" * 20)
    assert v.sniff_matches(".avi", b"RIFF\0\0\0\0AVI LIST")
    assert not v.sniff_matches(".mp4", b"MZ\x90\x00 this is an exe")
    assert not v.sniff_matches(".avi", b"RIFF\0\0\0\0WAVEfmt ")
    with pytest.raises(v.UploadValidationError):
        v.validate_signature(".mkv", b"not matroska at all")


def test_title_defaults_to_file_stem_and_is_length_limited():
    assert v.clean_title(None, "My Holiday.mp4") == "My Holiday"
    assert v.clean_title("  Hello \x07World ", "x.mp4") == "Hello World"
    with pytest.raises(v.UploadValidationError) as exc:
        v.clean_title("x" * 201, "x.mp4")
    assert exc.value.status_code == 422


# --------------------------------------------------------------------------- #
# upload service (direct, no gateway)
# --------------------------------------------------------------------------- #
@pytest.fixture
def upload_client(make_platform):
    p = make_platform(upload={"max_upload_bytes": 10_000})
    return p, p.direct["upload"]


def _post(client, data: bytes, filename="clip.mp4", ctype="video/mp4", **form):
    return client.post("/videos/upload", files={"file": (filename, io.BytesIO(data), ctype)}, data=form or None)


def test_valid_upload_persists_file_record_and_publishes_event(upload_client):
    p, client = upload_client
    payload = fake_mp4_bytes(4000)
    resp = _post(client, payload, title="Demo")
    assert resp.status_code == 201
    body = resp.json()
    assert body["title"] == "Demo" and body["size_bytes"] == 4000 and body["status"] == "uploaded"
    assert len(body["id"]) == 36 and len(body["checksum_sha256"]) == 64

    stored = p.storage / "uploads" / body["id"] / "original.mp4"
    assert stored.read_bytes() == payload
    assert not list(stored.parent.glob("*.part"))

    event = p.broker.messages("video.uploaded")[0]
    assert event["video_id"] == body["id"] and event["storage_path"] == f"uploads/{body['id']}/original.mp4"
    assert event["format"] == "mp4" and event["size_bytes"] == 4000


def test_every_upload_gets_a_unique_id(upload_client):
    _, client = upload_client
    ids = {_post(client, fake_mp4_bytes()).json()["id"] for _ in range(3)}
    assert len(ids) == 3


@pytest.mark.parametrize(
    "filename,ctype,data,status,code",
    [
        ("notes.txt", "text/plain", b"hello world", 415, "unsupported_media_type"),
        ("movie.mp4", "text/html", fake_mp4_bytes(), 415, "unsupported_media_type"),
        ("fake.mp4", "video/mp4", b"MZ\x90\x00" + b"\0" * 200, 415, "unsupported_media_type"),
        ("empty.mp4", "video/mp4", b"", 400, "empty_file"),
    ],
)
def test_invalid_uploads_are_rejected(upload_client, filename, ctype, data, status, code):
    p, client = upload_client
    resp = _post(client, data, filename=filename, ctype=ctype)
    assert resp.status_code == status
    assert resp.json()["error"]["code"] == code
    assert not list((p.storage / "uploads").glob("*"))  # nothing left behind
    assert p.broker.messages("video.uploaded") == []


def test_oversized_file_is_rejected_with_413_and_cleaned_up(upload_client):
    p, client = upload_client
    resp = _post(client, fake_mp4_bytes(10_001))  # limit is 10_000 bytes
    assert resp.status_code == 413
    assert resp.json()["error"]["code"] == "payload_too_large"
    assert not list((p.storage / "uploads").glob("*"))
    assert p.broker.messages("video.uploaded") == []


def test_file_exactly_at_the_limit_is_accepted(upload_client):
    _, client = upload_client
    assert _post(client, fake_mp4_bytes(10_000)).status_code == 201


def test_content_length_over_limit_is_rejected_before_reading_the_body(upload_client):
    _, client = upload_client
    big = fake_mp4_bytes(10_000 + 2 * 1024 * 1024)  # beyond limit + multipart overhead allowance
    resp = _post(client, big)
    assert resp.status_code == 413


def test_missing_file_field_is_422(upload_client):
    _, client = upload_client
    resp = client.post("/videos/upload", data={"title": "no file"}, files={"other": ("x.txt", io.BytesIO(b"x"), "text/plain")})
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"


def test_overlong_title_is_422(upload_client):
    _, client = upload_client
    assert _post(client, fake_mp4_bytes(), title="t" * 250).status_code == 422


def test_broker_outage_does_not_lose_the_upload_event(upload_client):
    p, client = upload_client
    p.broker.fail_publish = True
    resp = _post(client, fake_mp4_bytes())
    assert resp.status_code == 201  # the user's upload still succeeds
    assert p.broker.messages("video.uploaded") == []

    p.broker.fail_publish = False
    service = p.apps["upload"].state.service
    service.settings.republish_grace_seconds = 0
    assert service.republish_pending() == 1
    assert p.broker.messages("video.uploaded")[0]["video_id"] == resp.json()["id"]
    assert service.republish_pending() == 0  # idempotent: nothing pending any more
