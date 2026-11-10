from vp_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "orchestrator-service"
    metadata_service_url: str = "http://metadata:8000"
    http_timeout_seconds: float = 10.0

    # Retry policy
    max_attempts: int = 3
    retry_backoff_base_seconds: float = 5.0
    retry_backoff_max_seconds: float = 120.0

    # Resolutions produced when the caller does not specify any (CSV of heights)
    default_resolutions: str = "360,720,1080"

    # Scheduler
    scheduler_interval_seconds: float = 2.0
    job_stale_seconds: int = 300  # no heartbeat from the worker for this long => attempt failed
    republish_grace_seconds: float = 5.0

    @property
    def default_resolution_list(self) -> list[int]:
        return [int(x) for x in self.default_resolutions.split(",") if x.strip()]
