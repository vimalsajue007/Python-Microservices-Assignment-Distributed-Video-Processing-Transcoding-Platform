from vp_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "transcoder-service"

    ffmpeg_bin: str = "ffmpeg"
    ffprobe_bin: str = "ffprobe"
    ffmpeg_preset: str = "veryfast"
    ffmpeg_crf: int = 23
    transcode_timeout_seconds: int = 3600

    # Concurrent jobs handled by one worker process (scale out with more replicas)
    worker_concurrency: int = 1

    # Duplicate-message protection
    lock_ttl_seconds: int = 30  # lock is renewed while FFmpeg reports progress
    attempt_stale_seconds: int = 600  # fallback when Redis is down

    # Progress events
    progress_min_interval_seconds: float = 1.0
    progress_min_delta: int = 2

    # Chaos switch for demos: fail every attempt numbered <= N (0 disables)
    simulate_failure_attempts: int = 0
