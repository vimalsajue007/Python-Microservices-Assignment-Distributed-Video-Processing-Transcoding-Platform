"""Pure upload validation helpers (no I/O) so they are trivially unit-testable.

Three independent checks protect the platform from bad input:

1. the file extension must be on the allow-list,
2. the declared MIME type must be consistent with the extension,
3. the leading bytes must carry the container signature (magic number), so a
   renamed ``.exe`` or text file can not pass as ``.mp4``.
"""

from __future__ import annotations

import os
import re
from pathlib import PurePosixPath


class UploadValidationError(Exception):
    def __init__(self, status_code: int, code: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


ALLOWED_TYPES: dict[str, set[str]] = {
    ".mp4": {"video/mp4"},
    ".m4v": {"video/x-m4v", "video/mp4"},
    ".mov": {"video/quicktime"},
    ".mkv": {"video/x-matroska"},
    ".webm": {"video/webm"},
    ".avi": {"video/x-msvideo", "video/avi", "video/msvideo"},
}
ALLOWED_EXTENSIONS = tuple(sorted(ALLOWED_TYPES))
GENERIC_TYPES = {"application/octet-stream", "binary/octet-stream"}
SNIFF_BYTES = 64
MAX_TITLE_LENGTH = 200

_MP4_BRANDS = (b"ftyp", b"moov", b"mdat", b"wide", b"free", b"skip")
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")


def clean_filename(filename: str | None) -> str:
    name = os.path.basename((filename or "").replace("\\", "/")).strip()
    return _CONTROL_CHARS.sub("", name)[:255]


def validate_filename(filename: str | None) -> tuple[str, str]:
    """Return ``(clean_name, extension)`` or raise 415."""
    name = clean_filename(filename)
    if not name:
        raise UploadValidationError(422, "validation_error", "A file name is required")
    ext = PurePosixPath(name).suffix.lower()
    if ext not in ALLOWED_TYPES:
        raise UploadValidationError(
            415,
            "unsupported_media_type",
            f"Unsupported file type '{ext or 'none'}'. Allowed: {', '.join(ALLOWED_EXTENSIONS)}",
        )
    return name, ext


def validate_content_type(ext: str, content_type: str | None) -> str:
    declared = (content_type or "").split(";")[0].strip().lower()
    if not declared or declared in GENERIC_TYPES:
        return next(iter(sorted(ALLOWED_TYPES[ext])))
    if declared not in ALLOWED_TYPES[ext]:
        raise UploadValidationError(
            415,
            "unsupported_media_type",
            f"Content type '{declared}' does not match a '{ext}' video",
        )
    return declared


def sniff_matches(ext: str, header: bytes) -> bool:
    """Check container magic bytes against the claimed extension."""
    if ext in (".mp4", ".m4v", ".mov"):
        return len(header) >= 8 and header[4:8] in _MP4_BRANDS
    if ext in (".mkv", ".webm"):
        return header[:4] == b"\x1a\x45\xdf\xa3"  # EBML
    if ext == ".avi":
        return header[:4] == b"RIFF" and header[8:12] == b"AVI "
    return False


def validate_signature(ext: str, header: bytes) -> None:
    if not sniff_matches(ext, header):
        raise UploadValidationError(
            415,
            "unsupported_media_type",
            f"File content is not a valid '{ext}' video (signature mismatch)",
        )


def clean_title(title: str | None, filename: str) -> str:
    candidate = _CONTROL_CHARS.sub("", (title or "")).strip()
    if not candidate:
        candidate = PurePosixPath(filename).stem or "Untitled video"
    if len(candidate) > MAX_TITLE_LENGTH:
        raise UploadValidationError(
            422, "validation_error", f"Title must be at most {MAX_TITLE_LENGTH} characters"
        )
    return candidate
