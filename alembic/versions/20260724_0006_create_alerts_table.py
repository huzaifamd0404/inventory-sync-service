"""Create alerts table.

Revision ID: 20260724_0006_create_alerts_table
Revises: 20260723_0005_add_event_id_to_anomalies
Create Date: 2026-07-24 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260724_0006_create_alerts_table"
down_revision = "20260723_0005_add_event_id_to_anomalies"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create alerts table."""
    # Create alert_severity enum type
    alert_severity = sa.Enum("high", "critical", name="alert_severity")
    alert_severity.create(op.get_bind())

    # Create alert_status enum type
    alert_status = sa.Enum(
        "triggered", "acknowledged", "resolved", "suppressed", name="alert_status"
    )
    alert_status.create(op.get_bind())

    # Create alerts table
    op.create_table(
        "alerts",
        sa.Column("id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("anomaly_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("inventory_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.String(128), nullable=True),
        sa.Column("severity", alert_severity, nullable=False),
        sa.Column("status", alert_status, nullable=False, server_default="triggered"),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "triggered_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("acknowledged_by", sa.String(255), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("suppressed_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(["anomaly_id"], ["anomalies.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["inventory_id"], ["inventory.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create indexes
    op.create_index("ix_alerts_anomaly_triggered_at", "alerts", ["anomaly_id", "triggered_at"])
    op.create_index("ix_alerts_status_severity", "alerts", ["status", "severity"])
    op.create_index("ix_alerts_inventory_triggered_at", "alerts", ["inventory_id", "triggered_at"])
    op.create_index("ix_alerts_event_id", "alerts", ["event_id"], unique=True)


def downgrade() -> None:
    """Drop alerts table."""
    # Drop indexes
    op.drop_index("ix_alerts_event_id", table_name="alerts")
    op.drop_index("ix_alerts_inventory_triggered_at", table_name="alerts")
    op.drop_index("ix_alerts_status_severity", table_name="alerts")
    op.drop_index("ix_alerts_anomaly_triggered_at", table_name="alerts")

    # Drop table
    op.drop_table("alerts")

    # Drop enum types
    alert_status = sa.Enum(
        "triggered", "acknowledged", "resolved", "suppressed", name="alert_status"
    )
    alert_status.drop(op.get_bind())

    alert_severity = sa.Enum("high", "critical", name="alert_severity")
    alert_severity.drop(op.get_bind())
