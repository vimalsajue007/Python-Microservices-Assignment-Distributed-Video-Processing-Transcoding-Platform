"""Base settings shared by every service (12-factor: configuration via env vars)."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    service_name: str = "service"
    log_level: str = "INFO"

    # Infrastructure
    database_url: str | None = None
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/%2F"
    redis_url: str = "redis://redis:6379/0"
    storage_root: str = "/data"

    # Start consumers / schedulers inside the process (disabled in unit tests)
    enable_background: bool = True
    # Create tables from the ORM metadata (tests only; production uses Alembic)
    auto_create_schema: bool = False

    # Broker consumer behaviour
    consumer_max_redeliveries: int = 3
    consumer_retry_delay_ms: int = 5000
