"""Add processed events table for consumer idempotency

Revision ID: 20260714_0002
Revises: 20260708_0001
Create Date: 2026-07-14 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260714_0002"
down_revision: str | None = "20260708_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


processed_event_status = sa.Enum("processed", name="processed_event_status", create_constraint=False)

_pg_processed_event_status = postgresql.ENUM(
    "processed", name="processed_event_status", create_type=False
)


def upgrade() -> None:
    processed_event_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "processed_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.String(length=128), nullable=False),
        sa.Column("product_id", sa.String(length=128), nullable=False),
        sa.Column("store_id", sa.String(length=128), nullable=False),
        sa.Column("status", _pg_processed_event_status, nullable=False, server_default="processed"),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_processed_events"),
        sa.UniqueConstraint("event_id", name="uq_processed_events_event_id"),
    )
    op.create_index(
        "ix_processed_events_processed_at",
        "processed_events",
        ["processed_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_processed_events_processed_at", table_name="processed_events")
    op.drop_table("processed_events")

    processed_event_status.drop(op.get_bind(), checkfirst=True)
