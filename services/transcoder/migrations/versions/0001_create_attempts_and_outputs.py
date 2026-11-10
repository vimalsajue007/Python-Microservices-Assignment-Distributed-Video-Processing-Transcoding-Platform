"""create processing_attempts and transcoding_outputs

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
        "processing_attempts",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("job_id", sa.String(36), nullable=False),
        sa.Column("video_id", sa.String(36), nullable=False),
        sa.Column("attempt_number", sa.Integer, nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="running"),
        sa.Column("worker_id", sa.String(100)),
        sa.Column("message_id", sa.String(64)),
        sa.Column("error_type", sa.String(100)),
        sa.Column("error_message", sa.Text),
        sa.Column("duration_ms", sa.Integer),
        sa.Column("started_at", ts(), nullable=False),
        sa.Column("finished_at", ts()),
        sa.Column("created_at", ts(), nullable=False),
        sa.Column("updated_at", ts(), nullable=False),
        sa.UniqueConstraint("job_id", "attempt_number", name="uq_attempt_job_number"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_processing_attempts_video_id", "processing_attempts", ["video_id"])

    op.create_table(
        "transcoding_outputs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("video_id", sa.String(36), nullable=False),
        sa.Column("job_id", sa.String(36), nullable=False),
        sa.Column("attempt_id", sa.String(36), sa.ForeignKey("processing_attempts.id", ondelete="SET NULL")),
        sa.Column("resolution", sa.String(16), nullable=False),
        sa.Column("height", sa.Integer, nullable=False),
        sa.Column("width", sa.Integer, nullable=False),
        sa.Column("size_bytes", sa.BigInteger, nullable=False),
        sa.Column("video_codec", sa.String(32), nullable=False, server_default="h264"),
        sa.Column("relative_path", sa.String(512), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="completed"),
        sa.Column("created_at", ts(), nullable=False),
        sa.UniqueConstraint("video_id", "height", name="uq_output_video_height"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_transcoding_outputs_job_id", "transcoding_outputs", ["job_id"])


def downgrade() -> None:
    op.drop_index("ix_transcoding_outputs_job_id", table_name="transcoding_outputs")
    op.drop_table("transcoding_outputs")
    op.drop_index("ix_processing_attempts_video_id", table_name="processing_attempts")
    op.drop_table("processing_attempts")
