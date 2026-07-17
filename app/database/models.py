from __future__ import annotations

from datetime import datetime
from enum import Enum
from uuid import UUID, uuid4

from sqlalchemy import (
    DateTime,
    Enum as SqlEnum,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid as SqlUuid,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.database.base import Base


class InventoryChangeType(str, Enum):
    SYNC = "sync"
    ADJUSTMENT = "adjustment"
    SALE = "sale"
    RESTOCK = "restock"
    RETURN = "return"


class AnomalySeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AnomalyStatus(str, Enum):
    OPEN = "open"
    INVESTIGATING = "investigating"
    RESOLVED = "resolved"


class ProcessedEventStatus(str, Enum):
    PROCESSED = "processed"


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class Inventory(TimestampMixin, Base):
    __tablename__ = "inventory"
    __table_args__ = (
        UniqueConstraint("sku", "warehouse_id", name="uq_inventory_sku_warehouse"),
        Index("ix_inventory_sku_warehouse", "sku", "warehouse_id"),
        Index("ix_inventory_warehouse_active", "warehouse_id", "is_active"),
        Index("ix_inventory_updated_at", "updated_at"),
    )

    id: Mapped[UUID] = mapped_column(SqlUuid(as_uuid=True), primary_key=True, default=uuid4)
    sku: Mapped[str] = mapped_column(String(128), nullable=False)
    warehouse_id: Mapped[str] = mapped_column(String(128), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reorder_level: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_active: Mapped[bool] = mapped_column(default=True)

    history_entries: Mapped[list[InventoryHistory]] = relationship(
        back_populates="inventory",
        cascade="all, delete-orphan",
    )
    sales: Mapped[list[Sales]] = relationship(
        back_populates="inventory",
        cascade="all, delete-orphan",
    )
    anomalies: Mapped[list[Anomaly]] = relationship(
        back_populates="inventory",
        cascade="all, delete-orphan",
    )


class InventoryHistory(Base):
    __tablename__ = "inventory_history"
    __table_args__ = (
        Index("ix_inventory_history_inventory_changed_at", "inventory_id", "changed_at"),
        Index("ix_inventory_history_source_event", "source_event_id", unique=True),
    )

    id: Mapped[UUID] = mapped_column(SqlUuid(as_uuid=True), primary_key=True, default=uuid4)
    inventory_id: Mapped[UUID] = mapped_column(
        SqlUuid(as_uuid=True),
        ForeignKey("inventory.id", ondelete="CASCADE"),
        nullable=False,
    )
    change_type: Mapped[InventoryChangeType] = mapped_column(
        SqlEnum(InventoryChangeType, name="inventory_change_type"),
        nullable=False,
    )
    quantity_before: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity_after: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity_delta: Mapped[int] = mapped_column(Integer, nullable=False)
    source_event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    inventory: Mapped[Inventory] = relationship(back_populates="history_entries")


class Sales(Base):
    __tablename__ = "sales"
    __table_args__ = (
        Index("ix_sales_inventory_sold_at", "inventory_id", "sold_at"),
        Index("ix_sales_external_sale_id", "external_sale_id", unique=True),
    )

    id: Mapped[UUID] = mapped_column(SqlUuid(as_uuid=True), primary_key=True, default=uuid4)
    inventory_id: Mapped[UUID] = mapped_column(
        SqlUuid(as_uuid=True),
        ForeignKey("inventory.id", ondelete="CASCADE"),
        nullable=False,
    )
    quantity_sold: Mapped[int] = mapped_column(Integer, nullable=False)
    sale_price: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    external_sale_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    sold_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    inventory: Mapped[Inventory] = relationship(back_populates="sales")


class Anomaly(Base):
    __tablename__ = "anomalies"
    __table_args__ = (
        Index("ix_anomalies_inventory_detected_at", "inventory_id", "detected_at"),
        Index("ix_anomalies_status_severity", "status", "severity"),
    )

    id: Mapped[UUID] = mapped_column(SqlUuid(as_uuid=True), primary_key=True, default=uuid4)
    inventory_id: Mapped[UUID] = mapped_column(
        SqlUuid(as_uuid=True),
        ForeignKey("inventory.id", ondelete="CASCADE"),
        nullable=False,
    )
    anomaly_type: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[AnomalySeverity] = mapped_column(
        SqlEnum(AnomalySeverity, name="anomaly_severity"),
        nullable=False,
    )
    score: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[AnomalyStatus] = mapped_column(
        SqlEnum(AnomalyStatus, name="anomaly_status"),
        nullable=False,
        default=AnomalyStatus.OPEN,
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    inventory: Mapped[Inventory] = relationship(back_populates="anomalies")


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    __table_args__ = (Index("ix_processed_events_processed_at", "processed_at"),)

    id: Mapped[UUID] = mapped_column(SqlUuid(as_uuid=True), primary_key=True, default=uuid4)
    event_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    product_id: Mapped[str] = mapped_column(String(128), nullable=False)
    store_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[ProcessedEventStatus] = mapped_column(
        SqlEnum(ProcessedEventStatus, name="processed_event_status"),
        nullable=False,
        default=ProcessedEventStatus.PROCESSED,
    )
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class FailedEvent(Base):
    __tablename__ = "failed_events"
    __table_args__ = (
        UniqueConstraint(
            "source_topic",
            "source_partition",
            "source_offset",
            name="uq_failed_events_source_location",
        ),
        Index("ix_failed_events_event_id", "event_id"),
        Index("ix_failed_events_failed_at", "failed_at"),
    )

    id: Mapped[UUID] = mapped_column(SqlUuid(as_uuid=True), primary_key=True, default=uuid4)
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    source_offset: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    failure_reason: Mapped[str] = mapped_column(Text, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ReconciliationStatus(str, Enum):
    MATCH = "match"
    MISMATCH = "mismatch"
    MISSING = "missing"


class ReconciliationRecord(TimestampMixin, Base):
    """Persisted snapshot of a single reconciliation run for a product/store pair."""

    __tablename__ = "reconciliation_records"
    __table_args__ = (
        Index(
            "ix_reconciliation_records_store_product_at",
            "store_id",
            "product_id",
            "reconciled_at",
        ),
        Index("ix_reconciliation_records_status", "status"),
    )

    id: Mapped[UUID] = mapped_column(SqlUuid(as_uuid=True), primary_key=True, default=uuid4)
    store_id: Mapped[str] = mapped_column(String(128), nullable=False)
    product_id: Mapped[str] = mapped_column(String(128), nullable=False)
    expected_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    actual_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    difference: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[ReconciliationStatus] = mapped_column(
        SqlEnum(ReconciliationStatus, name="reconciliation_status"),
        nullable=False,
    )
    reconciled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


__all__ = [
    "Anomaly",
    "AnomalySeverity",
    "AnomalyStatus",
    "FailedEvent",
    "Inventory",
    "InventoryChangeType",
    "InventoryHistory",
    "ProcessedEvent",
    "ProcessedEventStatus",
    "ReconciliationRecord",
    "ReconciliationStatus",
    "Sales",
]
