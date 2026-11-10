"""Shared media storage layout.

Only *relative* paths are persisted/exchanged so the volume may be mounted at
different locations in different containers.

    uploads/<video_id>/original.<ext>
    outputs/<video_id>/<height>p.mp4
    thumbnails/<video_id>/thumb_<index>.jpg
"""

from __future__ import annotations

from pathlib import Path


class Storage:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def ensure_root(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    # relative paths -----------------------------------------------------
    @staticmethod
    def rel_upload(video_id: str, ext: str) -> str:
        return f"uploads/{video_id}/original{ext}"

    @staticmethod
    def rel_output(video_id: str, height: int) -> str:
        return f"outputs/{video_id}/{height}p.mp4"

    @staticmethod
    def rel_thumbnail(video_id: str, index: int) -> str:
        return f"thumbnails/{video_id}/thumb_{index}.jpg"

    # absolute paths -----------------------------------------------------
    def resolve(self, relative: str) -> Path:
        """Resolve a stored relative path, refusing anything outside the root."""
        root = self.root.resolve()
        path = (root / relative).resolve()
        if root != path and root not in path.parents:
            raise ValueError("path escapes storage root")
        return path

    def prepare(self, relative: str) -> Path:
        path = self.resolve(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
