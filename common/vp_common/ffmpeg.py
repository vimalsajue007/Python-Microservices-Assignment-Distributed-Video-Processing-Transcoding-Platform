"""Thin, testable wrappers around ffprobe / ffmpeg."""

from __future__ import annotations

import json
import logging
import subprocess
import threading
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

log = logging.getLogger("vp.ffmpeg")


class FFmpegError(Exception):
    """ffprobe/ffmpeg failed. ``retryable`` False means retrying cannot help."""

    def __init__(self, message: str, *, retryable: bool = True, stderr: str = ""):
        super().__init__(message)
        self.retryable = retryable
        self.stderr = stderr


@dataclass
class ProbeResult:
    duration: float
    width: int
    height: int
    container: str
    video_codec: str | None
    audio_codec: str | None
    bit_rate: int | None
    frame_rate: float | None
    size_bytes: int


def _parse_rate(value: str | None) -> float | None:
    if not value or value == "0/0":
        return None
    try:
        if "/" in value:
            num, den = value.split("/")
            return round(float(num) / float(den), 3) if float(den) else None
        return float(value)
    except ValueError:
        return None


def probe(path: str | Path, *, ffprobe_bin: str = "ffprobe", timeout: float = 60.0) -> ProbeResult:
    """Inspect a media file. Raises FFmpegError(retryable=False) for unreadable media."""
    path = str(path)
    cmd = [ffprobe_bin, "-v", "error", "-print_format", "json", "-show_format", "-show_streams", path]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError as exc:
        raise FFmpegError(f"{ffprobe_bin} is not installed", retryable=False) from exc
    except subprocess.TimeoutExpired as exc:
        raise FFmpegError("ffprobe timed out") from exc
    if proc.returncode != 0:
        raise FFmpegError(
            f"not a readable video file: {proc.stderr.strip()[-300:] or 'ffprobe failed'}",
            retryable=False,
            stderr=proc.stderr,
        )
    try:
        data = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise FFmpegError("ffprobe returned invalid JSON", retryable=False) from exc

    streams = data.get("streams", [])
    video = next((s for s in streams if s.get("codec_type") == "video"), None)
    audio = next((s for s in streams if s.get("codec_type") == "audio"), None)
    if video is None:
        raise FFmpegError("file contains no video stream", retryable=False)
    fmt = data.get("format", {})
    try:
        duration = float(fmt.get("duration") or video.get("duration") or 0)
    except (TypeError, ValueError):
        duration = 0.0
    bit_rate = fmt.get("bit_rate")
    return ProbeResult(
        duration=duration,
        width=int(video.get("width") or 0),
        height=int(video.get("height") or 0),
        container=str(fmt.get("format_name", "")).split(",")[0],
        video_codec=video.get("codec_name"),
        audio_codec=audio.get("codec_name") if audio else None,
        bit_rate=int(bit_rate) if bit_rate and str(bit_rate).isdigit() else None,
        frame_rate=_parse_rate(video.get("avg_frame_rate") or video.get("r_frame_rate")),
        size_bytes=int(fmt.get("size") or Path(path).stat().st_size),
    )


ProgressCallback = Callable[[float], None]


def transcode(
    source: str | Path,
    dest: str | Path,
    height: int,
    *,
    duration: float = 0.0,
    on_progress: ProgressCallback | None = None,
    ffmpeg_bin: str = "ffmpeg",
    preset: str = "veryfast",
    crf: int = 23,
    audio_bitrate: str = "128k",
    timeout: float = 3600.0,
) -> None:
    """Transcode to H.264/AAC MP4 scaled to ``height`` (width keeps aspect, even)."""
    cmd = [
        ffmpeg_bin, "-hide_banner", "-nostdin", "-y",
        "-i", str(source),
        "-map", "0:v:0", "-map", "0:a:0?",
        "-vf", f"scale=-2:{height}",
        "-c:v", "libx264", "-preset", preset, "-crf", str(crf), "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", audio_bitrate,
        "-movflags", "+faststart",
        "-f", "mp4",
        "-progress", "pipe:1", "-nostats",
        str(dest),
    ]
    _run_ffmpeg(cmd, duration=duration, on_progress=on_progress, timeout=timeout)


def extract_frame(
    source: str | Path,
    dest: str | Path,
    at_seconds: float,
    *,
    max_width: int = 640,
    ffmpeg_bin: str = "ffmpeg",
    timeout: float = 60.0,
) -> None:
    """Write a single JPEG frame taken at ``at_seconds``."""
    cmd = [
        ffmpeg_bin, "-hide_banner", "-nostdin", "-y",
        "-ss", f"{max(at_seconds, 0):.3f}", "-i", str(source),
        "-frames:v", "1",
        "-vf", f"scale='min({max_width},iw)':-2",
        "-q:v", "3", "-f", "image2",
        str(dest),
    ]
    _run_ffmpeg(cmd, timeout=timeout)
    p = Path(dest)
    if not p.exists() or p.stat().st_size == 0:
        raise FFmpegError("ffmpeg produced no frame at the requested position")


def _run_ffmpeg(
    cmd: list[str],
    *,
    duration: float = 0.0,
    on_progress: ProgressCallback | None = None,
    timeout: float = 3600.0,
) -> None:
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
        )
    except FileNotFoundError as exc:
        raise FFmpegError(f"{cmd[0]} is not installed", retryable=False) from exc

    stderr_tail: deque[str] = deque(maxlen=40)
    timed_out = threading.Event()

    def drain_stderr() -> None:
        assert proc.stderr is not None
        for line in proc.stderr:
            stderr_tail.append(line.rstrip())

    def kill() -> None:
        timed_out.set()
        proc.kill()

    reader = threading.Thread(target=drain_stderr, daemon=True)
    reader.start()
    timer = threading.Timer(timeout, kill)
    timer.start()
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            key, _, value = line.strip().partition("=")
            if on_progress and duration > 0 and key in ("out_time_us", "out_time_ms"):
                try:
                    seconds = int(value) / 1_000_000
                except ValueError:
                    continue
                on_progress(max(0.0, min(seconds / duration, 1.0)))
        returncode = proc.wait()
    finally:
        timer.cancel()
        reader.join(timeout=2)
    if timed_out.is_set():
        raise FFmpegError(f"ffmpeg timed out after {timeout:.0f}s")
    if returncode != 0:
        tail = "\n".join(stderr_tail)
        raise FFmpegError(f"ffmpeg exited with code {returncode}: {tail[-600:]}", stderr=tail)
    if on_progress:
        on_progress(1.0)
