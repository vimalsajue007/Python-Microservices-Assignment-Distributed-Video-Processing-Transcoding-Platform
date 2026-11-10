from vp_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "media-service"
    transcoder_service_url: str = "http://transcoder:8000"
    thumbnail_service_url: str = "http://thumbnail:8000"
    http_timeout_seconds: float = 10.0

    # Signed URL policy
    media_signing_secret: str = "change-me-in-production"
    signed_url_ttl_seconds: int = 3600
