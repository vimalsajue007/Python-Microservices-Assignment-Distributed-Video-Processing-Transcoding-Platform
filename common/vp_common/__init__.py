"""Shared building blocks for the video processing platform.

Every service depends on this package for configuration, messaging contracts,
database helpers, FFmpeg wrappers and a consistent HTTP error format. Services
never import each other: they only communicate through HTTP APIs and the
message contracts defined in :mod:`vp_common.events`.
"""

__version__ = "1.0.0"
