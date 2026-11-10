from __future__ import annotations

import pytest

from helpers.platform import build_platform
from helpers.videos import HAVE_FFMPEG, make_video


def pytest_collection_modifyitems(config, items):
    if HAVE_FFMPEG:
        return
    skip = pytest.mark.skip(reason="ffmpeg/ffprobe not installed")
    for item in items:
        if "ffmpeg" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def sample_video(tmp_path_factory):
    """A real 2 second 640x360 H.264/AAC mp4."""
    return make_video(tmp_path_factory.mktemp("samples") / "sample.mp4")


@pytest.fixture(scope="session")
def small_video(tmp_path_factory):
    """A 1 second 320x180 clip (smaller than 360p: exercises the no-upscale rule)."""
    return make_video(tmp_path_factory.mktemp("small") / "small.mp4", width=320, height=180, seconds=1)


@pytest.fixture
def make_platform(tmp_path):
    """Factory so a test can override service settings: make_platform(transcoder={...})."""
    created = []

    def factory(**overrides):
        p = build_platform(tmp_path, overrides)
        created.append(p)
        return p

    yield factory
    for p in created:
        p.gateway.close()
        for client in p.direct.values():
            client.close()


@pytest.fixture
def platform(make_platform):
    return make_platform()
