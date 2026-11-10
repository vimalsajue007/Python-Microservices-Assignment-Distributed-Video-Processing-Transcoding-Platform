.PHONY: up down logs test test-e2e dev-install demo-failures
up:            ; docker compose up -d --build
down:          ; docker compose down
logs:          ; docker compose logs -f --tail=100
dev-install:   ; pip install -e ./common -r requirements-dev.txt
test:          ; python -m pytest -q
# against a running stack (docker compose up -d --build)
test-e2e:      ; E2E_BASE_URL=http://localhost:8000 python -m pytest tests/e2e -v
# first two attempts of every job fail -> watch retries in the UI
demo-failures: ; SIMULATE_FAILURE_ATTEMPTS=2 docker compose up -d --build transcoder
