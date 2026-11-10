from __future__ import annotations

import logging
import sys


def configure_logging(level: str = "INFO", service: str = "service") -> None:
    root = logging.getLogger()
    if getattr(root, "_vp_configured", False):
        root.setLevel(level.upper())
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(f"%(asctime)s %(levelname)-7s [{service}] %(name)s: %(message)s")
    )
    root.handlers = [handler]
    root.setLevel(level.upper())
    root._vp_configured = True  # type: ignore[attr-defined]
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
