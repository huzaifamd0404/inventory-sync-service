"""Add event_id field to anomalies table.

Revision ID: 20260723_0005_add_event_id_to_anomalies
Revises: 20260717_0004_add_reconciliation_table
Create Date: 2026-07-23 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260723_0005_add_event_id_to_anomalies"
down_revision = "20260717_0004_add_reconciliation_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add event_id field and index to anomalies table."""
    # Add event_id column
    op.add_column(
        "anomalies",
        sa.Column("event_id", sa.String(128), nullable=True),
    )

    # Create unique index on event_id
    op.create_index(
        "ix_anomalies_event_id",
        "anomalies",
        ["event_id"],
        unique=True,
    )


def downgrade() -> None:
    """Remove event_id field and index from anomalies table."""
    # Drop unique index
    op.drop_index("ix_anomalies_event_id", table_name="anomalies")

    # Drop event_id column
    op.drop_column("anomalies", "event_id")
