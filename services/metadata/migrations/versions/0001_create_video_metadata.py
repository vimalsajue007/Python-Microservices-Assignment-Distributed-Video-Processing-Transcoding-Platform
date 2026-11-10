"""create video_metadata table

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
        "video_metadata",
        sa.Column("video_id", sa.String(36), primary_key=True),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("original_filename", sa.String(255), nullable=False),
        sa.Column("format", sa.String(16), nullable=False),
        sa.Column("container", sa.String(64)),
        sa.Column("video_codec", sa.String(32)),
        sa.Column("audio_codec", sa.String(32)),
        sa.Column("duration_seconds", sa.Float),
        sa.Column("width", sa.Integer),
        sa.Column("height", sa.Integer),
        sa.Column("frame_rate", sa.Float),
        sa.Column("bit_rate", sa.BigInteger),
        sa.Column("size_bytes", sa.BigInteger, nullable=False),
        sa.Column("storage_path", sa.String(512), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="uploaded"),
        sa.Column("analysis_status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("progress", sa.Integer, nullable=False, server_default="0"),
        sa.Column("latest_job_id", sa.String(36)),
        sa.Column("error_message", sa.Text),
        sa.Column("status_event_at", ts()),
        sa.Column("created_at", ts(), nullable=False),
        sa.Column("updated_at", ts(), nullable=False),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_video_metadata_status", "video_metadata", ["status"])
    op.create_index("ix_video_metadata_created_at", "video_metadata", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_video_metadata_created_at", table_name="video_metadata")
    op.drop_index("ix_video_metadata_status", table_name="video_metadata")
    op.drop_table("video_metadata")
