"""Media Delivery Service.

* ``GET /videos/{id}/outputs``     - available resolutions + signed media URLs
* ``GET /videos/{id}/thumbnails``  - thumbnail references + signed URLs
* ``GET /media/{kind}/{id}/{file}`` - streams a file after verifying the HMAC
  signature and expiry (supports HTTP Range requests for seeking in the player).

The service has **no database**: it composes data from the transcoder/thumbnail
services over HTTP and reads files from a read-only mount of the media volume.
"""

from __future__ import annotations

import logging
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Path, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

from vp_common.errors import UUID_PATTERN, APIError, install_error_handlers
from vp_common.http import ServiceClient
from vp_common.logging import configure_logging
from vp_common.runtime import health_router
from vp_common.security import sign, verify
from vp_common.storage import Storage

from .config import Settings

log = logging.getLogger("media")

KINDS = {"outputs": "video/mp4", "thumbnails": "image/jpeg"}
FILENAME_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,100}$")


class OutputMedia(BaseModel):
    resolution: str
    height: int
    width: int
    size_bytes: int
    content_type: str
    url: str
    expires_at: datetime


class OutputsResponse(BaseModel):
    video_id: str
    count: int
    outputs: list[OutputMedia]


class ThumbnailMedia(BaseModel):
    index: int
    timestamp_seconds: float | None
    width: int | None
    height: int | None
    status: str
    error_message: str | None
    url: str | None
    expires_at: datetime | None


class ThumbnailsResponse(BaseModel):
    video_id: str
    status: str
    count: int
    thumbnails: list[ThumbnailMedia]


def create_app(
    settings: Settings | None = None,
    *,
    transcoder_client: httpx.Client | None = None,
    thumbnail_client: httpx.Client | None = None,
) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.log_level, settings.service_name)

    storage = Storage(settings.storage_root)
    transcoder = ServiceClient(
        settings.transcoder_service_url, client=transcoder_client,
        timeout=settings.http_timeout_seconds, name="transcoder-service",
    )
    thumbnails = ServiceClient(
        settings.thumbnail_service_url, client=thumbnail_client,
        timeout=settings.http_timeout_seconds, name="thumbnail-service",
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        transcoder.close()
        thumbnails.close()

    app = FastAPI(title="Media Delivery Service", version="1.0.0", lifespan=lifespan)
    app.state.settings = settings
    install_error_handlers(app)
    app.include_router(health_router(settings.service_name))

    def signed_url(kind: str, video_id: str, filename: str) -> tuple[str, datetime]:
        path = f"/media/{kind}/{video_id}/{filename}"
        expires = int(time.time()) + settings.signed_url_ttl_seconds
        sig = sign(settings.media_signing_secret, path, expires)
        return f"{path}?expires={expires}&sig={sig}", datetime.fromtimestamp(expires, tz=timezone.utc)

    @app.get("/videos/{video_id}/outputs", response_model=OutputsResponse, tags=["media"])
    def get_outputs(video_id: str = Path(pattern=UUID_PATTERN)):
        data = transcoder.get_json(f"/internal/videos/{video_id}/outputs")
        outputs: list[OutputMedia] = []
        for item in data["outputs"]:
            filename = item["relative_path"].rsplit("/", 1)[-1]
            url, expires_at = signed_url("outputs", video_id, filename)
            outputs.append(
                OutputMedia(
                    resolution=item["resolution"], height=item["height"], width=item["width"],
                    size_bytes=item["size_bytes"], content_type=KINDS["outputs"], url=url, expires_at=expires_at,
                )
            )
        return OutputsResponse(video_id=video_id, count=len(outputs), outputs=outputs)

    @app.get("/videos/{video_id}/thumbnails", response_model=ThumbnailsResponse, tags=["media"])
    def get_thumbnails(video_id: str = Path(pattern=UUID_PATTERN)):
        data = thumbnails.get_json(f"/internal/videos/{video_id}/thumbnails")
        items: list[ThumbnailMedia] = []
        for item in data["items"]:
            url = expires_at = None
            if item["status"] == "completed" and item.get("relative_path"):
                url, expires_at = signed_url("thumbnails", video_id, item["relative_path"].rsplit("/", 1)[-1])
            items.append(
                ThumbnailMedia(
                    index=item["thumbnail_index"], timestamp_seconds=item["timestamp_seconds"],
                    width=item["width"], height=item["height"], status=item["status"],
                    error_message=item["error_message"], url=url, expires_at=expires_at,
                )
            )
        return ThumbnailsResponse(video_id=video_id, status=data["status"], count=len(items), thumbnails=items)

    @app.get("/media/{kind}/{video_id}/{filename}", tags=["media"], include_in_schema=True)
    def serve_media(
        kind: str,
        video_id: str = Path(pattern=UUID_PATTERN),
        filename: str = Path(),
        expires: int = Query(...),
        sig: str = Query(...),
    ):
        """Stream a media file. Requires a valid, unexpired signature."""
        if kind not in KINDS or not FILENAME_RE.match(filename) or ".." in filename:
            raise APIError(404, "not_found", "Media not found")
        canonical = f"/media/{kind}/{video_id}/{filename}"
        if not verify(settings.media_signing_secret, canonical, expires, sig):
            raise APIError(403, "invalid_signature", "The media link is invalid or has expired")
        try:
            path = storage.resolve(f"{kind}/{video_id}/{filename}")
        except ValueError:
            raise APIError(404, "not_found", "Media not found")
        if not path.is_file():
            raise APIError(404, "not_found", "Media not found")
        remaining = max(expires - int(time.time()), 0)
        return FileResponse(
            path, media_type=KINDS[kind],
            headers={"Cache-Control": f"private, max-age={min(remaining, 3600)}"},
        )

    return app
