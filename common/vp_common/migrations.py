"""Alembic helpers shared by every service that owns a database.

Each service keeps its own ``migrations/`` directory (service-owned schema).
The container entrypoint runs ``python -m vp_common.migrations`` which waits
for the database and upgrades it to ``head``.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

log = logging.getLogger("vp.migrations")


def alembic_config(service_dir: str | Path, url: str) -> Config:
    service_dir = Path(service_dir)
    cfg = Config()
    cfg.set_main_option("script_location", str(service_dir / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return cfg


def upgrade(service_dir: str | Path, url: str, revision: str = "head") -> None:
    command.upgrade(alembic_config(service_dir, url), revision)


def downgrade(service_dir: str | Path, url: str, revision: str = "base") -> None:
    command.downgrade(alembic_config(service_dir, url), revision)


def wait_for_db(url: str, timeout: float = 90.0) -> None:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            engine = create_engine(url, poolclass=NullPool)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2)
    raise RuntimeError(f"database not reachable after {timeout}s: {last}")


def run_env(context, target_metadata) -> None:
    """Body of every service's ``migrations/env.py``."""
    config = context.config
    url = config.get_main_option("sqlalchemy.url") or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("No database URL configured for migrations")

    if context.is_offline_mode():
        context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
        with context.begin_transaction():
            context.run_migrations()
        return

    engine = create_engine(url, poolclass=NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [migrate] %(message)s")
    url = os.environ.get("DATABASE_URL")
    if not url:
        log.error("DATABASE_URL is not set")
        sys.exit(1)
    service_dir = Path(os.environ.get("SERVICE_DIR", Path.cwd()))
    log.info("waiting for database ...")
    wait_for_db(url)
    log.info("running migrations from %s", service_dir / "migrations")
    upgrade(service_dir, url)
    log.info("migrations complete")


if __name__ == "__main__":
    main()
