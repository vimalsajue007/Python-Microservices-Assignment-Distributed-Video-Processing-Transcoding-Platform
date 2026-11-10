"""Migration scripts: apply cleanly, match the ORM models, and roll back."""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect

from vp_common.migrations import downgrade, upgrade

ROOT = Path(__file__).resolve().parents[2]

SERVICES = {
    "upload": ("upload_service.models", {"videos"}),
    "metadata": ("metadata_service.models", {"video_metadata"}),
    "orchestrator": ("orchestrator_service.models", {"processing_jobs"}),
    "transcoder": ("transcoder_service.models", {"processing_attempts", "transcoding_outputs"}),
    "thumbnail": ("thumbnail_service.models", {"video_thumbnails"}),
}


@pytest.mark.parametrize("service", SERVICES)
def test_upgrade_creates_exactly_the_service_owned_tables(service, tmp_path):
    models_module, tables = SERVICES[service]
    url = f"sqlite:///{tmp_path / 'm.db'}"
    upgrade(ROOT / "services" / service, url)
    inspector = inspect(create_engine(url))
    assert set(inspector.get_table_names()) - {"alembic_version"} == tables


@pytest.mark.parametrize("service", SERVICES)
def test_migrations_match_the_orm_models(service, tmp_path):
    """Guards against model/migration drift (columns, nullability, unique constraints)."""
    models_module, _ = SERVICES[service]
    base = importlib.import_module(models_module).Base
    url = f"sqlite:///{tmp_path / 'm.db'}"
    upgrade(ROOT / "services" / service, url)
    inspector = inspect(create_engine(url))
    for name, table in base.metadata.tables.items():
        migrated = {c["name"]: c for c in inspector.get_columns(name)}
        assert set(migrated) == {c.name for c in table.columns}, f"{name}: column mismatch"
        for column in table.columns:
            assert migrated[column.name]["nullable"] == column.nullable or column.primary_key, f"{name}.{column.name} nullability"
        model_uniques = {tuple(sorted(c.name for c in uc.columns)) for uc in table.constraints if uc.__class__.__name__ == "UniqueConstraint"}
        db_uniques = {tuple(sorted(u["column_names"])) for u in inspector.get_unique_constraints(name)}
        assert model_uniques <= db_uniques, f"{name}: missing unique constraint"


@pytest.mark.parametrize("service", SERVICES)
def test_downgrade_removes_everything(service, tmp_path):
    url = f"sqlite:///{tmp_path / 'm.db'}"
    upgrade(ROOT / "services" / service, url)
    downgrade(ROOT / "services" / service, url)
    assert set(inspect(create_engine(url)).get_table_names()) <= {"alembic_version"}


def test_upgrade_is_idempotent(tmp_path):
    url = f"sqlite:///{tmp_path / 'm.db'}"
    upgrade(ROOT / "services" / "orchestrator", url)
    upgrade(ROOT / "services" / "orchestrator", url)  # second run is a no-op


def test_every_required_entity_is_owned_by_exactly_one_service():
    required = {"videos", "video_metadata", "processing_jobs", "processing_attempts", "transcoding_outputs", "video_thumbnails"}
    owned = [t for _, tables in SERVICES.values() for t in tables]
    assert set(owned) == required and len(owned) == len(required)
