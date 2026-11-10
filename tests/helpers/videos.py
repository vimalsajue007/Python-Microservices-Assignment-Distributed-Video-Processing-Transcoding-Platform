"""Helpers that create small real media files with ffmpeg."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

HAVE_FFMPEG = shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None


def make_video(path: Path, *, width: int = 640, height: int = 360, seconds: float = 2.0, audio: bool = True, fmt: str | None = None) -> Path:
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
           "-f", "lavfi", "-i", f"testsrc=size={width}x{height}:rate=15:duration={seconds}"]
    if audio:
        cmd += ["-f", "lavfi", "-i", f"sine=frequency=440:duration={seconds}"]
    cmd += ["-c:v", "libx264", "-pix_fmt", "yuv420p", "-preset", "ultrafast"]
    if audio:
        cmd += ["-c:a", "aac", "-shortest"]
    if fmt:
        cmd += ["-f", fmt]
    cmd.append(str(path))
    subprocess.run(cmd, check=True, capture_output=True)
    return path


def fake_mp4_bytes(size: int = 2048) -> bytes:
    """Looks like an MP4 (valid ftyp box) but is not decodable media."""
    header = (24).to_bytes(4, "big") + b"ftypisom" + (0).to_bytes(4, "big") + b"isomiso2"
    return header + b"\x00" * max(size - len(header), 0)
