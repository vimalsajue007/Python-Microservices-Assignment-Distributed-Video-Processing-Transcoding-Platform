"""Unit tests: API gateway (routing, request validation, error handling)."""

from __future__ import annotations

import io

import httpx
import pytest
from fastapi.testclient import TestClient

from gateway_service.config import Settings
from gateway_service.main import create_app
from helpers.fakes import DownTransport

VIDEO_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def gateway_with(handler, **settings) -> TestClient:
    app = create_app(Settings(**settings), http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def seen():
    return []


@pytest.fixture
def gw(seen):
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"ok": True, "path": request.url.path})

    return gateway_with(handler)


# --------------------------------------------------------------------------- #
# routing
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "method,path,service_host",
    [
        ("GET", "/videos", "metadata"),
        ("GET", f"/videos/{VIDEO_ID}", "metadata"),
        ("GET", f"/videos/{VIDEO_ID}/status", "metadata"),
        ("POST", f"/videos/{VIDEO_ID}/process", "orchestrator"),
        ("GET", f"/videos/{VIDEO_ID}/jobs", "orchestrator"),
        ("GET", f"/jobs/{VIDEO_ID}", "orchestrator"),
        ("POST", f"/jobs/{VIDEO_ID}/retry", "orchestrator"),
        ("GET", f"/videos/{VIDEO_ID}/outputs", "media"),
        ("GET", f"/videos/{VIDEO_ID}/thumbnails", "media"),
    ],
)
def test_routes_requests_to_the_owning_service(gw, seen, method, path, service_host):
    resp = gw.request(method, path)
    assert resp.status_code in (200, 202)
    assert seen[-1].url.host == service_host and seen[-1].url.path == path
    assert seen[-1].headers["x-request-id"]  # propagated for tracing


def test_upload_is_streamed_to_the_upload_service(seen):
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request, request.read()))
        return httpx.Response(201, json={"id": "x"})

    client = gateway_with(handler)
    resp = client.post("/videos/upload", files={"file": ("clip.mp4", io.BytesIO(b"abc" * 100), "video/mp4")}, data={"title": "T"})
    assert resp.status_code == 201 and resp.json() == {"id": "x"}
    request, body = seen[0]
    assert request.url.host == "upload" and request.url.path == "/videos/upload"
    assert request.headers["content-type"].startswith("multipart/form-data; boundary=")
    assert b"abc" * 100 in body and b'name="title"' in body


def test_upstream_status_and_body_are_passed_through(seen):
    client = gateway_with(lambda r: httpx.Response(409, json={"error": {"code": "job_in_progress", "message": "busy"}}))
    resp = client.post(f"/videos/{VIDEO_ID}/process")
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "job_in_progress"


def test_query_parameters_are_forwarded(gw, seen):
    gw.get("/videos", params={"status": "completed", "q": "demo", "limit": 5, "offset": 10})
    q = dict(seen[-1].url.params)
    assert q == {"status": "completed", "q": "demo", "limit": "5", "offset": "10"}


def test_process_body_is_forwarded_without_nulls(gw, seen):
    gw.post(f"/videos/{VIDEO_ID}/process", json={"resolutions": [360, 720]})
    import json

    assert json.loads(seen[-1].content) == {"resolutions": [360, 720]}
    gw.post(f"/videos/{VIDEO_ID}/process")
    assert json.loads(seen[-1].content) == {}


# --------------------------------------------------------------------------- #
# validation (rejected before any upstream call)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "method,path,kwargs",
    [
        ("GET", "/videos/not-a-uuid", {}),
        ("GET", "/videos/123/status", {}),
        ("GET", "/jobs/%20", {}),
        ("POST", "/videos/abc/process", {}),
        ("GET", "/videos", {"params": {"limit": 0}}),
        ("GET", "/videos", {"params": {"limit": 1000}}),
        ("GET", "/videos", {"params": {"offset": -1}}),
        ("GET", "/videos", {"params": {"status": "weird"}}),
        ("POST", f"/videos/{VIDEO_ID}/process", {"json": {"resolutions": [999]}}),
        ("POST", f"/videos/{VIDEO_ID}/process", {"json": {"resolutions": []}}),
        ("POST", f"/videos/{VIDEO_ID}/process", {"json": {"resolutions": "720"}}),
        ("POST", f"/videos/{VIDEO_ID}/process", {"json": {"max_attempts": 99}}),
    ],
)
def test_invalid_requests_get_422_and_never_reach_a_service(gw, seen, method, path, kwargs):
    resp = gw.request(method, path, **kwargs)
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"]["code"] == "validation_error" and body["error"]["details"] and body["request_id"]
    assert seen == []


def test_upload_must_be_multipart(gw, seen):
    resp = gw.post("/videos/upload", json={"file": "nope"})
    assert resp.status_code == 415 and resp.json()["error"]["code"] == "unsupported_media_type"
    assert seen == []


def test_oversized_upload_is_rejected_at_the_edge(seen):
    client = gateway_with(lambda r: seen.append(r) or httpx.Response(201, json={}), max_upload_bytes=1000, multipart_overhead_bytes=500)
    resp = client.post("/videos/upload", files={"file": ("a.mp4", io.BytesIO(b"x" * 5000), "video/mp4")})
    assert resp.status_code == 413 and resp.json()["error"]["code"] == "payload_too_large"
    assert seen == []


def test_unknown_route_and_wrong_method_use_the_error_envelope(gw):
    nf = gw.get("/nope")
    assert nf.status_code == 404 and nf.json()["error"]["code"] == "not_found"
    mna = gw.delete(f"/videos/{VIDEO_ID}")
    assert mna.status_code == 405 and mna.json()["error"]["code"] == "method_not_allowed"


def test_media_route_validates_kind_and_signature_shape(gw):
    assert gw.get(f"/media/secrets/{VIDEO_ID}/a.mp4?expires=1&sig=" + "a" * 32).status_code == 422
    assert gw.get(f"/media/outputs/{VIDEO_ID}/a.mp4").status_code == 422
    assert gw.get(f"/media/outputs/{VIDEO_ID}/a.mp4?expires=1&sig=short").status_code == 422


def test_media_range_headers_are_forwarded_and_partial_content_streams_back(seen):
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(206, content=b"0123456789", headers={"content-range": "bytes 0-9/100", "content-type": "video/mp4", "accept-ranges": "bytes"})

    client = gateway_with(handler)
    resp = client.get(f"/media/outputs/{VIDEO_ID}/360p.mp4?expires=2000000000&sig=" + "a" * 64, headers={"Range": "bytes=0-9"})
    assert resp.status_code == 206 and resp.content == b"0123456789"
    assert resp.headers["content-range"] == "bytes 0-9/100" and resp.headers["accept-ranges"] == "bytes"
    assert seen[0].headers["range"] == "bytes=0-9"


# --------------------------------------------------------------------------- #
# common error handling
# --------------------------------------------------------------------------- #
def test_service_down_is_a_502_with_the_standard_envelope():
    app = create_app(Settings(), http_client=httpx.AsyncClient(transport=DownTransport()))
    resp = TestClient(app, raise_server_exceptions=False).get("/videos")
    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "upstream_unavailable" and resp.json()["request_id"]


def test_service_timeout_is_a_504():
    def handler(request):
        raise httpx.ReadTimeout("slow", request=request)

    resp = gateway_with(handler).get("/videos")
    assert resp.status_code == 504 and resp.json()["error"]["code"] == "upstream_timeout"


def test_invalid_upstream_response_is_a_502():
    def handler(request):
        raise httpx.RemoteProtocolError("garbage", request=request)

    assert gateway_with(handler).get("/videos").status_code == 502


def test_request_id_is_echoed_and_generated(gw):
    assert gw.get("/videos", headers={"X-Request-ID": "trace-123"}).headers["x-request-id"] == "trace-123"
    assert len(gw.get("/videos").headers["x-request-id"]) >= 16


def test_cors_headers_are_present(gw):
    resp = gw.options("/videos", headers={"Origin": "http://localhost:3000", "Access-Control-Request-Method": "GET"})
    assert resp.status_code == 200 and resp.headers["access-control-allow-origin"]


def test_health_endpoints(gw):
    assert gw.get("/health").json() == {"status": "ok", "service": "gateway"}
    agg = gw.get("/health/services").json()
    assert agg["status"] == "ok" and set(agg["services"]) == {"upload", "metadata", "orchestrator", "transcoder", "thumbnail", "media"}


def test_aggregated_health_reports_down_services():
    app = create_app(Settings(), http_client=httpx.AsyncClient(transport=DownTransport()))
    agg = TestClient(app).get("/health/services").json()
    assert agg["status"] == "degraded" and set(agg["services"].values()) == {"down"}


def test_openapi_documents_the_required_endpoints(gw):
    paths = gw.get("/openapi.json").json()["paths"]
    for p in ["/videos/upload", "/videos", "/videos/{video_id}", "/videos/{video_id}/status",
              "/videos/{video_id}/process", "/jobs/{job_id}", "/videos/{video_id}/outputs", "/videos/{video_id}/thumbnails"]:
        assert p in paths, p
    assert "multipart/form-data" in paths["/videos/upload"]["post"]["requestBody"]["content"]
