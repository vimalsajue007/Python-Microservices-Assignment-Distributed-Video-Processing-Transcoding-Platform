"""create processing_jobs table

Revision ID: 0001
Revises:
Create Date: 2026-11-02
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def ts() -> sa.types.TypeEngine:
    return sa.DateTime().with_variant(mysql.DATETIME(fsp=6), "mysql")


def upgrade() -> None:
    op.create_table(
        "processing_jobs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("video_id", sa.String(36), nullable=False),
        sa.Column("job_type", sa.String(32), nullable=False, server_default="transcode"),
        sa.Column("status", sa.String(16), nullable=False, server_default="queued"),
        sa.Column("progress", sa.Integer, nullable=False, server_default="0"),
        sa.Column("requested_resolutions", sa.JSON, nullable=False),
        sa.Column("source_path", sa.String(512), nullable=False),
        sa.Column("max_attempts", sa.Integer, nullable=False, server_default="3"),
        sa.Column("current_attempt", sa.Integer, nullable=False, server_default="1"),
        sa.Column("attempt_history", sa.JSON, nullable=False),
        sa.Column("last_error", sa.Text),
        sa.Column("next_retry_at", ts()),
        sa.Column("last_published_at", ts()),
        sa.Column("heartbeat_at", ts()),
        sa.Column("started_at", ts()),
        sa.Column("completed_at", ts()),
        sa.Column("dead_lettered_at", ts()),
        sa.Column("created_at", ts(), nullable=False),
        sa.Column("updated_at", ts(), nullable=False),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_processing_jobs_video_id", "processing_jobs", ["video_id"])
    op.create_index("ix_processing_jobs_status", "processing_jobs", ["status"])
    op.create_index("ix_processing_jobs_next_retry_at", "processing_jobs", ["next_retry_at"])


def downgrade() -> None:
    op.drop_index("ix_processing_jobs_next_retry_at", table_name="processing_jobs")
    op.drop_index("ix_processing_jobs_status", table_name="processing_jobs")
    op.drop_index("ix_processing_jobs_video_id", table_name="processing_jobs")
    op.drop_table("processing_jobs")
