from vp_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "metadata-service"
    ffprobe_bin: str = "ffprobe"
