# syntax=docker/dockerfile:1
# Build context: repository root  ->  docker build -f services/transcoder/Dockerfile .
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY common /app/common
RUN pip install /app/common
COPY services/transcoder /app/service
WORKDIR /app/service
ENV PYTHONPATH=/app/service
RUN useradd --system --uid 10001 vp && mkdir -p /data && chown vp /data
USER vp
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 CMD curl -fsS http://localhost:8000/health || exit 1
# apply this service's Alembic migrations (waits for MySQL), then serve
CMD ["sh", "-c", "python -m vp_common.migrations && exec uvicorn transcoder_service.main:create_app --factory --host 0.0.0.0 --port 8000"]
