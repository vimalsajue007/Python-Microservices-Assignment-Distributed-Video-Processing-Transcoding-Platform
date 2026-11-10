"""create video_thumbnails table

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
        "video_thumbnails",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("video_id", sa.String(36), nullable=False),
        sa.Column("thumbnail_index", sa.Integer, nullable=False),
        sa.Column("timestamp_seconds", sa.Float),
        sa.Column("relative_path", sa.String(512)),
        sa.Column("width", sa.Integer),
        sa.Column("height", sa.Integer),
        sa.Column("size_bytes", sa.BigInteger),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text),
        sa.Column("created_at", ts(), nullable=False),
        sa.Column("updated_at", ts(), nullable=False),
        sa.UniqueConstraint("video_id", "thumbnail_index", name="uq_thumbnail_video_index"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_video_thumbnails_video_id", "video_thumbnails", ["video_id"])


def downgrade() -> None:
    op.drop_index("ix_video_thumbnails_video_id", table_name="video_thumbnails")
    op.drop_table("video_thumbnails")
