"""API Gateway: the single entry point for the frontend.

Responsibilities
* route each public endpoint to the owning service,
* validate request formats (UUIDs, query bounds, multipart uploads, process body),
* translate upstream failures into one consistent error format (502/503/504),
* propagate ``X-Request-ID`` for traceability,
* stream uploads and media without buffering them in memory.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import Body, FastAPI, Path, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field, field_validator
from starlette.background import BackgroundTask

from vp_common.errors import UUID_PATTERN, APIError, install_error_handlers
from vp_common.events import ALLOWED_RESOLUTIONS
from vp_common.logging import configure_logging
from vp_common.runtime import health_router

from .config import Settings

log = logging.getLogger("gateway")

PASS_HEADERS = {
    "content-type", "cache-control", "etag", "last-modified",
    "content-range", "accept-ranges", "content-disposition",
}
STREAM_EXTRA_HEADERS = {"content-length", "content-encoding"}

UPLOAD_OPENAPI = {
    "requestBody": {
        "required": True,
        "content": {
            "multipart/form-data": {
                "schema": {
                    "type": "object",
                    "required": ["file"],
                    "properties": {
                        "file": {"type": "string", "format": "binary", "description": "mp4, m4v, mov, mkv, webm or avi"},
                        "title": {"type": "string", "maxLength": 200},
                    },
                }
            }
        },
    }
}


class ProcessRequest(BaseModel):
    resolutions: list[int] | None = Field(default=None, max_length=8, description="Heights to produce")
    max_attempts: int | None = Field(default=None, ge=1, le=10)

    @field_validator("resolutions")
    @classmethod
    def _allowed(cls, value: list[int] | None):
        if value is None:
            return value
        bad = [v for v in value if v not in ALLOWED_RESOLUTIONS]
        if bad:
            raise ValueError(f"unsupported resolution(s) {bad}; allowed: {list(ALLOWED_RESOLUTIONS)}")
        if not value:
            raise ValueError("resolutions must not be empty")
        return value


def create_app(settings: Settings | None = None, *, http_client: httpx.AsyncClient | None = None) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.log_level, settings.service_name)
    client = http_client or httpx.AsyncClient(
        timeout=httpx.Timeout(settings.request_timeout_seconds, connect=5.0),
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await client.aclose()

    app = FastAPI(
        title="Video Platform API Gateway",
        version="1.0.0",
        description="Single entry point for the Distributed Video Processing & Transcoding Platform.",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origin_list or ["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Request-ID", "Content-Range", "Accept-Ranges", "Content-Length"],
    )
    install_error_handlers(app)
    app.include_router(health_router(settings.service_name))

    # ------------------------------------------------------------------ #
    async def forward(
        request: Request,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        content: Any = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        stream: bool = False,
    ) -> Response:
        req_headers = {"x-request-id": getattr(request.state, "request_id", "")}
        req_headers.update(headers or {})
        try:
            upstream_request = client.build_request(
                method, url, params=params, json=json, content=content, headers=req_headers,
                timeout=timeout if timeout is not None else httpx.USE_CLIENT_DEFAULT,
            )
            upstream = await client.send(upstream_request, stream=stream)
        except httpx.ConnectError as exc:
            log.warning("upstream unreachable %s: %s", url, exc)
            raise APIError(502, "upstream_unavailable", "A required backend service is unavailable") from exc
        except httpx.TimeoutException as exc:
            raise APIError(504, "upstream_timeout", "The backend service took too long to respond") from exc
        except httpx.HTTPError as exc:
            log.warning("upstream error %s: %s", url, exc)
            raise APIError(502, "upstream_error", "The backend service returned an invalid response") from exc

        allowed = PASS_HEADERS | (STREAM_EXTRA_HEADERS if stream else set())
        out_headers = {k: v for k, v in upstream.headers.items() if k.lower() in allowed}
        if stream:
            if upstream.is_stream_consumed:  # already buffered (e.g. in-process transports)
                out_headers.pop("content-encoding", None)

                async def buffered():
                    yield upstream.content

                body = buffered()
            else:
                body = upstream.aiter_raw()
            return StreamingResponse(
                body, status_code=upstream.status_code, headers=out_headers,
                background=BackgroundTask(upstream.aclose),
            )
        return Response(content=upstream.content, status_code=upstream.status_code, headers=out_headers)

    # ------------------------------------------------------------------ #
    # Required API surface
    # ------------------------------------------------------------------ #
    @app.post("/videos/upload", status_code=201, tags=["videos"], openapi_extra=UPLOAD_OPENAPI,
              summary="Upload a video (multipart/form-data)")
    async def upload_video(request: Request):
        content_type = request.headers.get("content-type", "")
        if not content_type.lower().startswith("multipart/form-data"):
            raise APIError(415, "unsupported_media_type", "Uploads must use multipart/form-data")
        raw_length = request.headers.get("content-length")
        if raw_length is None:
            raise APIError(411, "length_required", "Content-Length header is required")
        try:
            length = int(raw_length)
        except ValueError:
            raise APIError(400, "bad_request", "Invalid Content-Length header")
        if length <= 0:
            raise APIError(400, "empty_file", "Request body is empty")
        if length > settings.max_upload_bytes + settings.multipart_overhead_bytes:
            raise APIError(
                413, "payload_too_large",
                f"File exceeds the maximum size of {settings.max_upload_bytes // (1024 * 1024)} MiB",
            )
        return await forward(
            request, "POST", f"{settings.upload_service_url}/videos/upload",
            content=request.stream(),
            headers={"content-type": content_type, "content-length": raw_length},
            timeout=settings.upload_timeout_seconds,
        )

    @app.get("/videos", tags=["videos"], summary="List videos with metadata and status")
    async def list_videos(
        request: Request,
        status: str | None = Query(default=None, pattern="^(uploaded|queued|processing|completed|failed|invalid)$"),
        q: str | None = Query(default=None, max_length=100),
        limit: int = Query(default=50, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
    ):
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        if q:
            params["q"] = q
        return await forward(request, "GET", f"{settings.metadata_service_url}/videos", params=params)

    @app.get("/videos/{video_id}", tags=["videos"], summary="Get video metadata")
    async def get_video(request: Request, video_id: str = Path(pattern=UUID_PATTERN)):
        return await forward(request, "GET", f"{settings.metadata_service_url}/videos/{video_id}")

    @app.get("/videos/{video_id}/status", tags=["videos"], summary="Get processing status")
    async def get_video_status(request: Request, video_id: str = Path(pattern=UUID_PATTERN)):
        return await forward(request, "GET", f"{settings.metadata_service_url}/videos/{video_id}/status")

    @app.post("/videos/{video_id}/process", status_code=202, tags=["processing"],
              summary="Start asynchronous processing (transcoding)")
    async def process_video(
        request: Request,
        video_id: str = Path(pattern=UUID_PATTERN),
        body: ProcessRequest | None = Body(default=None),
    ):
        payload = body.model_dump(exclude_none=True) if body else {}
        return await forward(
            request, "POST", f"{settings.orchestrator_service_url}/videos/{video_id}/process", json=payload
        )

    @app.get("/videos/{video_id}/jobs", tags=["processing"], summary="List processing jobs of a video")
    async def list_video_jobs(request: Request, video_id: str = Path(pattern=UUID_PATTERN)):
        return await forward(request, "GET", f"{settings.orchestrator_service_url}/videos/{video_id}/jobs")

    @app.get("/jobs/{job_id}", tags=["processing"], summary="Get job status, retries and attempt history")
    async def get_job(request: Request, job_id: str = Path(pattern=UUID_PATTERN)):
        return await forward(request, "GET", f"{settings.orchestrator_service_url}/jobs/{job_id}")

    @app.post("/jobs/{job_id}/retry", status_code=202, tags=["processing"], summary="Retry a failed job")
    async def retry_job(request: Request, job_id: str = Path(pattern=UUID_PATTERN)):
        return await forward(request, "POST", f"{settings.orchestrator_service_url}/jobs/{job_id}/retry")

    @app.get("/videos/{video_id}/outputs", tags=["media"], summary="Available resolutions and media URLs")
    async def get_outputs(request: Request, video_id: str = Path(pattern=UUID_PATTERN)):
        return await forward(request, "GET", f"{settings.media_service_url}/videos/{video_id}/outputs")

    @app.get("/videos/{video_id}/thumbnails", tags=["media"], summary="Thumbnails with signed URLs")
    async def get_thumbnails(request: Request, video_id: str = Path(pattern=UUID_PATTERN)):
        return await forward(request, "GET", f"{settings.media_service_url}/videos/{video_id}/thumbnails")

    @app.get("/media/{kind}/{video_id}/{filename}", tags=["media"], summary="Stream a signed media file")
    async def stream_media(
        request: Request,
        kind: str = Path(pattern="^(outputs|thumbnails)$"),
        video_id: str = Path(pattern=UUID_PATTERN),
        filename: str = Path(pattern=r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,100}$"),
        expires: int = Query(...),
        sig: str = Query(..., min_length=16, max_length=128),
    ):
        passthrough = {
            k: v for k, v in request.headers.items()
            if k.lower() in ("range", "if-range", "if-none-match", "if-modified-since")
        }
        return await forward(
            request, "GET", f"{settings.media_service_url}/media/{kind}/{video_id}/{filename}",
            params={"expires": expires, "sig": sig}, headers=passthrough, stream=True,
            timeout=settings.upload_timeout_seconds,
        )

    # ------------------------------------------------------------------ #
    @app.get("/health/services", tags=["health"], summary="Aggregated health of all backend services")
    async def services_health():
        targets = {
            "upload": settings.upload_service_url,
            "metadata": settings.metadata_service_url,
            "orchestrator": settings.orchestrator_service_url,
            "transcoder": settings.transcoder_service_url,
            "thumbnail": settings.thumbnail_service_url,
            "media": settings.media_service_url,
        }

        async def check(name: str, url: str) -> tuple[str, str]:
            try:
                r = await client.get(f"{url}/health", timeout=2.0)
                return name, "up" if r.status_code == 200 else "degraded"
            except httpx.HTTPError:
                return name, "down"

        results = dict(await asyncio.gather(*(check(n, u) for n, u in targets.items())))
        overall = "ok" if all(v == "up" for v in results.values()) else "degraded"
        return {"status": overall, "services": results}

    return app
