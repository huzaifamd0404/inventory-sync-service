"""Add reconciliation_records table

Revision ID: 20260717_0004
Revises: 20260716_0003
Create Date: 2026-07-17 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260717_0004"
down_revision: str | None = "20260716_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


reconciliation_status = sa.Enum(
    "match",
    "mismatch",
    "missing",
    name="reconciliation_status",
    create_constraint=False,
)

_pg_reconciliation_status = postgresql.ENUM(
    "match",
    "mismatch",
    "missing",
    name="reconciliation_status",
    create_type=False,
)


def upgrade() -> None:
    reconciliation_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "reconciliation_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("store_id", sa.String(length=128), nullable=False),
        sa.Column("product_id", sa.String(length=128), nullable=False),
        sa.Column("expected_quantity", sa.Integer(), nullable=False),
        sa.Column("actual_quantity", sa.Integer(), nullable=False),
        sa.Column("difference", sa.Integer(), nullable=False),
        sa.Column("status", _pg_reconciliation_status, nullable=False),
        sa.Column(
            "reconciled_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_reconciliation_records"),
    )
    op.create_index(
        "ix_reconciliation_records_store_product_at",
        "reconciliation_records",
        ["store_id", "product_id", "reconciled_at"],
        unique=False,
    )
    op.create_index(
        "ix_reconciliation_records_status",
        "reconciliation_records",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_reconciliation_records_status", table_name="reconciliation_records")
    op.drop_index(
        "ix_reconciliation_records_store_product_at", table_name="reconciliation_records"
    )
    op.drop_table("reconciliation_records")
    reconciliation_status.drop(op.get_bind(), checkfirst=True)
