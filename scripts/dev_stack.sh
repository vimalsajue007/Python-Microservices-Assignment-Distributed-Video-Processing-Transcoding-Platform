#!/usr/bin/env bash
# Runs all seven services as plain processes (no Docker) against locally
# installed MySQL 8, RabbitMQ and Redis. Handy for debugging and for CI machines
# without a Docker daemon. The Docker Compose stack is the primary way to run the platform.
#
#   scripts/dev_stack.sh start     # create schemas, run migrations, start services
#   scripts/dev_stack.sh stop
#   scripts/dev_stack.sh status
#
# Requirements: python3.12+, `pip install -e ./common`, ffmpeg/ffprobe on PATH.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${VP_RUN_DIR:-/tmp/vp-dev}"
LOG_DIR="$RUN_DIR/logs"
mkdir -p "$LOG_DIR" "$RUN_DIR/data"

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
export RABBITMQ_URL="${RABBITMQ_URL:-amqp://guest:guest@127.0.0.1:5672/%2F}"
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
export STORAGE_ROOT="${STORAGE_ROOT:-$RUN_DIR/data}"
export MEDIA_SIGNING_SECRET="${MEDIA_SIGNING_SECRET:-dev-signing-secret}"
export PYTHONPATH="$ROOT/common${PYTHONPATH:+:$PYTHONPATH}"
export SIMULATE_FAILURE_ATTEMPTS="${SIMULATE_FAILURE_ATTEMPTS:-0}"
export MAX_UPLOAD_BYTES="${MAX_UPLOAD_BYTES:-524288000}"

# name:port
SERVICES=(upload:8001 metadata:8002 orchestrator:8003 transcoder:8004 thumbnail:8005 media:8006 gateway:8000)
DB_SERVICES=(upload metadata orchestrator transcoder thumbnail)

db_url() { echo "mysql+pymysql://vp_$1:${1}_pw@${MYSQL_HOST}:${MYSQL_PORT}/vp_$1?charset=utf8mb4"; }

start() {
  echo "==> creating databases and users (infra/mysql/init.sh)"
  bash "$ROOT/infra/mysql/init.sh"
  for svc in "${DB_SERVICES[@]}"; do
    echo "==> migrating $svc"
    (cd "$ROOT/services/$svc" && DATABASE_URL="$(db_url "$svc")" python3 -m vp_common.migrations >"$LOG_DIR/migrate-$svc.log" 2>&1)
  done
  for entry in "${SERVICES[@]}"; do
    name="${entry%%:*}"; port="${entry##*:}"
    echo "==> starting $name on :$port"
    env DATABASE_URL="$( [[ " ${DB_SERVICES[*]} " == *" $name "* ]] && db_url "$name" || echo "")" \
        UPLOAD_SERVICE_URL=http://127.0.0.1:8001 METADATA_SERVICE_URL=http://127.0.0.1:8002 \
        ORCHESTRATOR_SERVICE_URL=http://127.0.0.1:8003 TRANSCODER_SERVICE_URL=http://127.0.0.1:8004 \
        THUMBNAIL_SERVICE_URL=http://127.0.0.1:8005 MEDIA_SERVICE_URL=http://127.0.0.1:8006 \
        PYTHONPATH="$ROOT/common:$ROOT/services/$name" \
        nohup python3 -m uvicorn "${name}_service.main:create_app" --factory --host 127.0.0.1 --port "$port" \
        >"$LOG_DIR/$name.log" 2>&1 &
    echo $! >"$RUN_DIR/$name.pid"
  done
  echo "==> waiting for the gateway"
  for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:8000/health/services" 2>/dev/null | grep -q '"status":"ok"'; then
      echo "stack is up: http://127.0.0.1:8000/docs  (logs: $LOG_DIR)"; return 0
    fi
    sleep 1
  done
  echo "stack did not become healthy; see $LOG_DIR" >&2; exit 1
}

stop() {
  for entry in "${SERVICES[@]}"; do
    name="${entry%%:*}"
    if [[ -f "$RUN_DIR/$name.pid" ]]; then kill "$(cat "$RUN_DIR/$name.pid")" 2>/dev/null || true; rm -f "$RUN_DIR/$name.pid"; fi
  done
  echo "stopped"
}

status() { curl -fsS http://127.0.0.1:8000/health/services || echo "gateway not reachable"; echo; }

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  *) echo "usage: $0 {start|stop|status}"; exit 2 ;;
esac
