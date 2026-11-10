from vp_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "thumbnail-service"

    ffmpeg_bin: str = "ffmpeg"
    ffprobe_bin: str = "ffprobe"
    thumbnail_count: int = 3
    thumbnail_max_width: int = 640
    ffmpeg_attempts: int = 2  # in-process attempts per thumbnail before it is marked failed
    lock_ttl_seconds: int = 120
