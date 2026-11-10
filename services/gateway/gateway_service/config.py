from vp_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "gateway"

    upload_service_url: str = "http://upload:8000"
    metadata_service_url: str = "http://metadata:8000"
    orchestrator_service_url: str = "http://orchestrator:8000"
    transcoder_service_url: str = "http://transcoder:8000"
    thumbnail_service_url: str = "http://thumbnail:8000"
    media_service_url: str = "http://media:8000"

    # Request limits (the upload service enforces the same limit authoritatively)
    max_upload_bytes: int = 500 * 1024 * 1024
    multipart_overhead_bytes: int = 1024 * 1024

    request_timeout_seconds: float = 30.0
    upload_timeout_seconds: float = 900.0
    cors_origins: str = "*"  # comma separated

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]
