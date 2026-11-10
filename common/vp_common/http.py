"""Synchronous HTTP client used for service-to-service calls (the sync API contract)."""

from __future__ import annotations

from typing import Any

import httpx

from .errors import APIError


class ServiceClient:
    """Wraps ``httpx.Client`` and maps transport failures to platform API errors.

    ``client`` can be injected so tests can mount another service's ASGI app.
    """

    def __init__(self, base_url: str | None = None, *, client: httpx.Client | None = None, timeout: float = 10.0, name: str = "upstream"):
        self.name = name
        self._client = client or httpx.Client(base_url=base_url or "", timeout=timeout)

    def get_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        try:
            resp = self._client.get(path, params=params)
        except httpx.TimeoutException as exc:
            raise APIError(504, "upstream_timeout", f"{self.name} timed out") from exc
        except httpx.HTTPError as exc:
            raise APIError(503, "upstream_unavailable", f"{self.name} is unavailable") from exc
        if resp.status_code == 404:
            raise APIError(404, "not_found", "resource not found")
        if resp.status_code >= 500:
            raise APIError(502, "upstream_error", f"{self.name} returned {resp.status_code}")
        if resp.status_code >= 400:
            raise APIError(resp.status_code, "upstream_rejected", f"{self.name} rejected the request")
        return resp.json()

    def close(self) -> None:
        self._client.close()
