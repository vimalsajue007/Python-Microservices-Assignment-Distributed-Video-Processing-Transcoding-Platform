"""Wires all seven services together in one process.

* every service gets its **own** SQLite database (service-owned data),
* one shared :class:`InMemoryBroker` replaces RabbitMQ (same delivery semantics),
* :class:`FakeRedis` replaces Redis,
* service-to-service HTTP calls are routed to the other apps' ASGI interfaces,
* the API gateway is reached with a regular ``TestClient``.

Everything else (validation, FFmpeg, SQL, message contracts) is the real code.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from gateway_service.config import Settings as GatewaySettings
from gateway_service.main import create_app as create_gateway
from media_service.config import Settings as MediaSettings
from media_service.main import create_app as create_media
from metadata_service.config import Settings as MetadataSettings
from metadata_service.main import create_app as create_metadata
from orchestrator_service.config import Settings as OrchestratorSettings
from orchestrator_service.main import create_app as create_orchestrator
from thumbnail_service.config import Settings as ThumbnailSettings
from thumbnail_service.main import create_app as create_thumbnail
from transcoder_service.config import Settings as TranscoderSettings
from transcoder_service.main import create_app as create_transcoder
from upload_service.config import Settings as UploadSettings
from upload_service.main import create_app as create_upload
from vp_common.messaging import InMemoryBroker

from .fakes import FakeRedis, HostRouterTransport


def _client(app: FastAPI, host: str) -> TestClient:
    return TestClient(app, base_url=f"http://{host}", raise_server_exceptions=False)


@dataclass
class Platform:
    broker: InMemoryBroker
    redis: FakeRedis
    storage: Path
    apps: dict[str, FastAPI]
    gateway: TestClient
    direct: dict[str, TestClient]
    secret: str = "test-secret"

    # ---- convenience ------------------------------------------------------
    @property
    def orchestrator(self):
        return self.apps["orchestrator"].state.service

    @property
    def worker(self):
        return self.apps["transcoder"].state.worker

    def upload(self, content: bytes | Path, filename: str = "clip.mp4", content_type: str = "video/mp4", title: str | None = None):
        data = content.read_bytes() if isinstance(content, Path) else content
        form = {"title": title} if title is not None else None
        return self.gateway.post("/videos/upload", files={"file": (filename, io.BytesIO(data), content_type)}, data=form)

    def process(self, video_id: str, **body: Any):
        return self.gateway.post(f"/videos/{video_id}/process", json=body or None)

    def run_scheduler(self, max_ticks: int = 20) -> int:
        """Drive the orchestrator scheduler until it has nothing left to do."""
        total = 0
        for _ in range(max_ticks):
            stats = self.orchestrator.tick()
            moved = sum(stats.values())
            total += moved
            if not moved:
                break
        return total

    def job(self, job_id: str) -> dict:
        return self.gateway.get(f"/jobs/{job_id}").json()

    def video(self, video_id: str) -> dict:
        return self.gateway.get(f"/videos/{video_id}").json()


def build_platform(tmp_path: Path, overrides: dict[str, dict[str, Any]] | None = None) -> Platform:
    overrides = overrides or {}
    storage = tmp_path / "media"
    storage.mkdir(parents=True, exist_ok=True)
    broker = InMemoryBroker()
    fake_redis = FakeRedis()
    common = dict(enable_background=False, auto_create_schema=True, storage_root=str(storage))

    def db(name: str) -> str:
        return f"sqlite:///{tmp_path / (name + '.db')}"

    def cfg(cls, name: str, **kwargs):
        merged = {**common, **kwargs, **overrides.get(name, {})}
        if name in ("upload", "metadata", "orchestrator", "transcoder", "thumbnail"):
            merged.setdefault("database_url", db(name))
        return cls(**merged)

    metadata = create_metadata(cfg(MetadataSettings, "metadata"), broker=broker)
    upload = create_upload(cfg(UploadSettings, "upload", max_upload_bytes=overrides.get("upload", {}).get("max_upload_bytes", 5 * 1024 * 1024)), broker=broker)
    transcoder = create_transcoder(
        cfg(TranscoderSettings, "transcoder", ffmpeg_preset="ultrafast"), broker=broker, redis_client=fake_redis
    )
    thumbnail = create_thumbnail(cfg(ThumbnailSettings, "thumbnail"), broker=broker, redis_client=fake_redis)
    orchestrator = create_orchestrator(
        cfg(OrchestratorSettings, "orchestrator", retry_backoff_base_seconds=0.0, retry_backoff_max_seconds=0.0, republish_grace_seconds=0.0),
        broker=broker, metadata_client=_client(metadata, "metadata"), redis_client=fake_redis,
    )
    media = create_media(
        cfg(MediaSettings, "media", media_signing_secret="test-secret"),
        transcoder_client=_client(transcoder, "transcoder"), thumbnail_client=_client(thumbnail, "thumbnail"),
    )

    apps = {
        "upload": upload, "metadata": metadata, "orchestrator": orchestrator,
        "transcoder": transcoder, "thumbnail": thumbnail, "media": media,
    }
    transport = HostRouterTransport(apps)
    gateway_settings = GatewaySettings(
        service_name="gateway", max_upload_bytes=overrides.get("upload", {}).get("max_upload_bytes", 5 * 1024 * 1024),
        **overrides.get("gateway", {}),
    )
    gateway = create_gateway(gateway_settings, http_client=httpx.AsyncClient(transport=transport))
    apps["gateway"] = gateway

    # Register consumers (what each service's lifespan does in production)
    for name in ("metadata", "orchestrator", "transcoder", "thumbnail"):
        apps[name].state.start_consumers()

    return Platform(
        broker=broker, redis=fake_redis, storage=storage, apps=apps,
        gateway=TestClient(gateway, raise_server_exceptions=False),
        direct={n: _client(a, n) for n, a in apps.items()},
    )
