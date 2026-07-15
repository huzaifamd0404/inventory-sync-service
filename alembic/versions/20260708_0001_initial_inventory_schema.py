"""Initial inventory domain schema

Revision ID: 20260708_0001
Revises:
Create Date: 2026-07-08 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260708_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


inventory_change_type = sa.Enum(
    "sync",
    "adjustment",
    "sale",
    "restock",
    "return",
    name="inventory_change_type",
    create_constraint=False,
)

anomaly_severity = sa.Enum(
    "low",
    "medium",
    "high",
    "critical",
    name="anomaly_severity",
    create_constraint=False,
)

anomaly_status = sa.Enum(
    "open",
    "investigating",
    "resolved",
    name="anomaly_status",
    create_constraint=False,
)

# Using postgresql.ENUM with create_type=False prevents SQLAlchemy from
# emitting CREATE TYPE again during create_table; types are created
# explicitly in upgrade() with checkfirst=True.
_pg_change_type = postgresql.ENUM(
    "sync", "adjustment", "sale", "restock", "return",
    name="inventory_change_type", create_type=False,
)
_pg_severity = postgresql.ENUM(
    "low", "medium", "high", "critical",
    name="anomaly_severity", create_type=False,
)
_pg_status = postgresql.ENUM(
    "open", "investigating", "resolved",
    name="anomaly_status", create_type=False,
)


def upgrade() -> None:
    inventory_change_type.create(op.get_bind(), checkfirst=True)
    anomaly_severity.create(op.get_bind(), checkfirst=True)
    anomaly_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "inventory",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sku", sa.String(length=128), nullable=False),
        sa.Column("warehouse_id", sa.String(length=128), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("reorder_level", sa.Integer(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
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
        sa.PrimaryKeyConstraint("id", name="pk_inventory"),
        sa.UniqueConstraint("sku", "warehouse_id", name="uq_inventory_sku_warehouse"),
    )
    op.create_index(
        "ix_inventory_sku_warehouse", "inventory", ["sku", "warehouse_id"], unique=False
    )
    op.create_index("ix_inventory_updated_at", "inventory", ["updated_at"], unique=False)
    op.create_index(
        "ix_inventory_warehouse_active", "inventory", ["warehouse_id", "is_active"], unique=False
    )

    op.create_table(
        "anomalies",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("inventory_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("anomaly_type", sa.String(length=64), nullable=False),
        sa.Column("severity", _pg_severity, nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("status", _pg_status, nullable=False, server_default="open"),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "detected_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["inventory_id"],
            ["inventory.id"],
            name="fk_anomalies_inventory_id_inventory",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_anomalies"),
    )
    op.create_index(
        "ix_anomalies_inventory_detected_at",
        "anomalies",
        ["inventory_id", "detected_at"],
        unique=False,
    )
    op.create_index(
        "ix_anomalies_status_severity", "anomalies", ["status", "severity"], unique=False
    )

    op.create_table(
        "inventory_history",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("inventory_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("change_type", _pg_change_type, nullable=False),
        sa.Column("quantity_before", sa.Integer(), nullable=False),
        sa.Column("quantity_after", sa.Integer(), nullable=False),
        sa.Column("quantity_delta", sa.Integer(), nullable=False),
        sa.Column("source_event_id", sa.String(length=128), nullable=True),
        sa.Column(
            "changed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["inventory_id"],
            ["inventory.id"],
            name="fk_inventory_history_inventory_id_inventory",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_inventory_history"),
    )
    op.create_index(
        "ix_inventory_history_inventory_changed_at",
        "inventory_history",
        ["inventory_id", "changed_at"],
        unique=False,
    )
    op.create_index(
        "ix_inventory_history_source_event",
        "inventory_history",
        ["source_event_id"],
        unique=True,
    )

    op.create_table(
        "sales",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("inventory_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("quantity_sold", sa.Integer(), nullable=False),
        sa.Column("sale_price", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("external_sale_id", sa.String(length=128), nullable=True),
        sa.Column(
            "sold_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["inventory_id"],
            ["inventory.id"],
            name="fk_sales_inventory_id_inventory",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_sales"),
    )
    op.create_index("ix_sales_external_sale_id", "sales", ["external_sale_id"], unique=True)
    op.create_index(
        "ix_sales_inventory_sold_at", "sales", ["inventory_id", "sold_at"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_sales_inventory_sold_at", table_name="sales")
    op.drop_index("ix_sales_external_sale_id", table_name="sales")
    op.drop_table("sales")

    op.drop_index("ix_inventory_history_source_event", table_name="inventory_history")
    op.drop_index("ix_inventory_history_inventory_changed_at", table_name="inventory_history")
    op.drop_table("inventory_history")

    op.drop_index("ix_anomalies_status_severity", table_name="anomalies")
    op.drop_index("ix_anomalies_inventory_detected_at", table_name="anomalies")
    op.drop_table("anomalies")

    op.drop_index("ix_inventory_warehouse_active", table_name="inventory")
    op.drop_index("ix_inventory_updated_at", table_name="inventory")
    op.drop_index("ix_inventory_sku_warehouse", table_name="inventory")
    op.drop_table("inventory")

    anomaly_status.drop(op.get_bind(), checkfirst=True)
    anomaly_severity.drop(op.get_bind(), checkfirst=True)
    inventory_change_type.drop(op.get_bind(), checkfirst=True)
