from vp_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "upload-service"
    max_upload_bytes: int = 500 * 1024 * 1024  # 500 MiB
    # multipart framing overhead tolerated on top of max_upload_bytes when checking Content-Length
    multipart_overhead_bytes: int = 1024 * 1024
    republish_interval_seconds: float = 10.0
    republish_grace_seconds: float = 5.0
