"""Add failed events table for persistent DLQ tracking

Revision ID: 20260716_0003
Revises: 20260714_0002
Create Date: 2026-07-16 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260716_0003"
down_revision: str | None = "20260714_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "failed_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.String(length=128), nullable=True),
        sa.Column("source_topic", sa.String(length=255), nullable=False),
        sa.Column("source_partition", sa.Integer(), nullable=False),
        sa.Column("source_offset", sa.Integer(), nullable=False),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("failure_reason", sa.Text(), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "failed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_failed_events"),
        sa.UniqueConstraint(
            "source_topic",
            "source_partition",
            "source_offset",
            name="uq_failed_events_source_location",
        ),
    )
    op.create_index("ix_failed_events_event_id", "failed_events", ["event_id"], unique=False)
    op.create_index("ix_failed_events_failed_at", "failed_events", ["failed_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_failed_events_failed_at", table_name="failed_events")
    op.drop_index("ix_failed_events_event_id", table_name="failed_events")
    op.drop_table("failed_events")
