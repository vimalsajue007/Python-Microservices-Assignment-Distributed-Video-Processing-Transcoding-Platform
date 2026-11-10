# Aurum — Distributed Video Processing & Transcoding Platform

Seven independently deployable Python microservices (FastAPI · SQLAlchemy · MySQL 8 · RabbitMQ · Redis · FFmpeg) and a React + TypeScript dashboard in a premium yellow theme.

* **Architecture & sync/async diagram:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · [docs/architecture.svg](docs/architecture.svg)
* **API reference:** [docs/API.md](docs/API.md) (+ live Swagger at `/docs`)

```
frontend/            React + TS + Vite, served by nginx (proxies /api and /media to the gateway)
common/vp_common/    shared library: config, logging, DB, migrations, RabbitMQ topology + broker, locks, FFmpeg, signing, errors
services/
  gateway/ upload/ metadata/ orchestrator/ transcoder/ thumbnail/ media/     (each: code, Dockerfile, migrations/)
infra/mysql/init.sh  one database + one DB user per service
tests/               unit/ integration/ e2e/
docker-compose.yml   full stack
```

## Run it (Docker)

```bash
cp .env.example .env          # optional, defaults work
docker compose up -d --build
```
| | URL |
|---|---|
| Dashboard | http://localhost:3000 |
| API gateway + Swagger | http://localhost:8000/docs |
| RabbitMQ console | http://localhost:15672 (guest / guest) |

Migrations run automatically when each service container starts. Scale encoders with `docker compose up -d --scale transcoder=3`.

### Try the failure handling
```bash
SIMULATE_FAILURE_ATTEMPTS=2 docker compose up -d --build transcoder
```
The first two attempts of every job fail; open a video in the UI and watch the attempt timeline (failed → failed → succeeded). Set the value to `99` to exhaust retries: the job is dead-lettered (see `transcode.jobs.dlq` in the RabbitMQ console) and the UI offers **Retry job**.

## Run without Docker
Needs Python 3.12+, FFmpeg, MySQL 8, RabbitMQ, Redis running locally.
```bash
pip install -e ./common -r requirements-dev.txt
scripts/dev_stack.sh start     # creates DBs, migrates, starts the 7 services (gateway :8000)
cd frontend && npm install && npm run dev      # http://localhost:3000 (proxies to :8000)
scripts/dev_stack.sh stop
```

## Tests
```bash
pip install -e ./common -r requirements-dev.txt     # needs ffmpeg on PATH
python -m pytest -q                                  # 200 tests: unit + integration (all 7 services in-process)

# end-to-end against a running stack (Docker or dev_stack.sh)
E2E_BASE_URL=http://localhost:8000 python -m pytest tests/e2e -v
#   + E2E_MAX_UPLOAD_MB=<limit>            oversize-upload test
#   + E2E_EXPECT_FAILURES=1                (stack started with SIMULATE_FAILURE_ATTEMPTS=2) worker-retry test
```
| Layer | Covers |
|---|---|
| `tests/unit` | upload validation (formats, magic bytes, size, title), job creation/retry/backoff/stale logic, worker idempotency, messaging/DLQ, signed media, gateway, thumbnails, metadata, migrations |
| `tests/integration` | complete upload → metadata/thumbnails → transcode → playback, no-upscale, non-blocking API, worker failures + retries, retry limit → DLQ → manual retry, duplicate messages, one active job per video, stale worker |
| `tests/e2e` | the same journey against the real Compose stack (RabbitMQ, MySQL, Redis, FFmpeg), plus invalid/oversized uploads and error contract |

Integration tests wire all seven services in-process with one SQLite DB per service, real FFmpeg, and in-memory broker/Redis fakes that mimic at-least-once delivery. Migrations are additionally checked against MySQL 8.

## Configuration (env vars)
| Variable | Default | Meaning |
|---|---|---|
| `MAX_UPLOAD_BYTES` | 524288000 | upload size limit (gateway + upload service) |
| `MEDIA_SIGNING_SECRET` | `change-me-in-production` | HMAC key for signed media URLs — **change it** |
| `SIMULATE_FAILURE_ATTEMPTS` | 0 | chaos switch for the worker |
| `MAX_ATTEMPTS` / `RETRY_BACKOFF_BASE_SECONDS` / `RETRY_BACKOFF_MAX_SECONDS` | 3 / 5 / 120 | orchestrator retry policy |
| `JOB_STALE_SECONDS` | 300 | heartbeat timeout before an attempt is failed |
| `CONSUMER_MAX_REDELIVERIES` / `CONSUMER_RETRY_DELAY_MS` | 3 / 5000 | poison-message handling |
| `SIGNED_URL_TTL_SECONDS` | 3600 | media URL lifetime |

## Requirement checklist
| Requirement | Where |
|---|---|
| 7 services, independently deployable | `services/*` each with its own Dockerfile |
| Service-owned DBs, entities, migrations | `infra/mysql/init.sh`, `services/*/*/models.py`, `services/*/migrations` |
| All required endpoints | [docs/API.md](docs/API.md) |
| React dashboard: upload, list, status/progress, failures/retries, resolutions, thumbnails, playback | `frontend/src` |
| Async messaging, retries, DLQ, duplicates, validation, non-blocking API, stored errors | [Reliability design](docs/ARCHITECTURE.md#reliability-design) |
| Tests (unit, integration, invalid/oversized, duplicates/failures, e2e) | `tests/` |
| Dockerfiles, Compose, run instructions, architecture diagram | this repo |
