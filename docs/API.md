# API reference

Base URL (Compose): `http://localhost:8000` (the UI reaches it as `/api`). Interactive OpenAPI docs: **`/docs`** (Swagger UI) and `/openapi.json` on the gateway.
All IDs are UUIDv4. All timestamps are ISO-8601 UTC. Every response carries `X-Request-ID`.

## Error envelope

```json
{ "error": { "code": "unsupported_media_type", "message": "…", "details": null }, "request_id": "9f2c…" }
```

| HTTP | `code` | When |
|---|---|---|
| 400 | `bad_request`, `empty_file` | malformed request / zero-byte upload |
| 403 | `invalid_signature` | media URL tampered with or expired |
| 404 | `video_not_found`, `job_not_found`, `not_found` | unknown resource |
| 409 | `job_in_progress`, `job_not_failed`, `video_not_ready` | state conflicts (`details.job_id` for an active job) |
| 411 | `length_required` | upload without `Content-Length` |
| 413 | `payload_too_large` | file larger than `MAX_UPLOAD_BYTES` (default 500 MiB) |
| 415 | `unsupported_media_type` | extension / content-type / magic-bytes check failed |
| 422 | `validation_error`, `video_not_processable` | invalid parameters; video could not be decoded |
| 502/504 | `upstream_error`, `upstream_unavailable`, `upstream_timeout` | a backend service failed or timed out |

## Videos

### `POST /videos/upload` → 201
`multipart/form-data`: `file` (required; `.mp4 .m4v .mov .mkv .webm .avi`), `title` (optional, ≤ 200 chars).

```json
{ "id": "…", "title": "Launch teaser", "original_filename": "teaser.mp4", "content_type": "video/mp4",
  "size_bytes": 13003, "checksum_sha256": "…", "status": "uploaded", "created_at": "…" }
```
Metadata and thumbnails are produced asynchronously right after the upload.

### `GET /videos`
Query: `status` (`uploaded|queued|processing|completed|failed|invalid`), `q` (title search), `limit` (1-100, default 50), `offset`.
```json
{ "items": [ { "video_id": "…", "title": "…", "status": "completed", "progress": 100, "width": 1280, "height": 720,
               "duration_seconds": 1.5, "video_codec": "h264", "audio_codec": "aac", "format": "mp4",
               "size_bytes": 13003, "latest_job_id": "…", "error_message": null, "created_at": "…", "updated_at": "…" } ],
  "total": 1, "limit": 50, "offset": 0 }
```

### `GET /videos/{id}`
One item of the list above (full metadata). `404 video_not_found`.

### `GET /videos/{id}/status`
```json
{ "video_id": "…", "status": "processing", "progress": 42, "job_id": "…", "error_message": null, "updated_at": "…" }
```

## Processing

### `POST /videos/{id}/process` → 202
Optional body: `{ "resolutions": [360, 720, 1080], "max_attempts": 3 }` (defaults: `360,720,1080` / `3`). Heights above the source are skipped. Returns the **Job** immediately (`status: queued`).
Errors: 404, 409 `job_in_progress`, 422 `video_not_processable`, 422 `validation_error` (unsupported resolution).

### `GET /jobs/{id}`
```json
{ "id": "…", "video_id": "…", "job_type": "transcode", "status": "retrying", "progress": 0,
  "resolutions": [360, 720], "attempt": 1, "max_attempts": 3, "retries_remaining": 2,
  "last_error": "TranscodeError: Simulated failure on attempt 1",
  "attempt_history": [ { "attempt": 1, "status": "failed", "worker_id": "vm-8621", "started_at": "…", "finished_at": "…", "error": "…" } ],
  "next_retry_at": "…", "dead_lettered": false, "created_at": "…", "updated_at": "…", "started_at": "…", "completed_at": null }
```
Job `status`: `queued → running → completed`, or `retrying` (backoff pending) / `failed` (retries exhausted; `dead_lettered: true`).

### `GET /videos/{id}/jobs` · `POST /jobs/{id}/retry` → 202
List jobs of a video; retry a `failed` job with a fresh attempt budget (`409 job_not_failed` otherwise).

## Outputs & playback

### `GET /videos/{id}/outputs`
```json
{ "video_id": "…", "count": 2, "outputs": [
  { "resolution": "360p", "height": 360, "width": 640, "size_bytes": 31000, "content_type": "video/mp4",
    "url": "/media/outputs/<id>/360p.mp4?expires=1767225600&sig=…", "expires_at": "…" } ] }
```

### `GET /videos/{id}/thumbnails`
```json
{ "video_id": "…", "status": "completed", "count": 3,
  "thumbnails": [ { "index": 0, "timestamp_seconds": 0.4, "width": 640, "height": 360, "status": "completed", "url": "/media/thumbnails/<id>/thumb_0.jpg?expires=…&sig=…" } ] }
```
Aggregate `status`: `none | processing | completed | partial | failed`.

### `GET /media/{kind}/{video_id}/{filename}`
Streams a file (`kind` = `outputs` | `thumbnails`). Requires the signed `expires` + `sig` query (HMAC-SHA256, default TTL 1 h). Supports `Range` (→ 206) so `<video>` seeking works. Bad/expired signature → 403.

## Operations

`GET /health/services` → `{ "status": "ok", "services": { "upload": "ok", … } }` · every service also exposes `/health` and `/ready`.
Each internal service also serves its own OpenAPI docs at `/docs` (Compose: not published; use `scripts/dev_stack.sh` or `docker compose exec`).
