from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.database.base import Base
from app.database.models import (
    Inventory,
    InventoryHistory,
    InventoryChangeType,
    ReconciliationRecord,
    ReconciliationStatus,
)
from app.services.reconciliation_service import (
    ReconciliationResult,
    ReconciliationService,
    ReconciliationTransientError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_session_factory() -> sessionmaker[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def seed_inventory(
    session: Session,
    *,
    sku: str,
    warehouse_id: str,
    quantity: int,
) -> Inventory:
    inv = Inventory(sku=sku, warehouse_id=warehouse_id, quantity=quantity)
    session.add(inv)
    session.flush()
    return inv


def seed_history(
    session: Session,
    *,
    inventory_id: UUID,
    delta: int,
    quantity_before: int,
    quantity_after: int,
    change_type: InventoryChangeType = InventoryChangeType.RESTOCK,
) -> InventoryHistory:
    entry = InventoryHistory(
        inventory_id=inventory_id,
        change_type=change_type,
        quantity_before=quantity_before,
        quantity_after=quantity_after,
        quantity_delta=delta,
    )
    session.add(entry)
    session.flush()
    return entry


# ---------------------------------------------------------------------------
# Unit tests – ReconciliationService
# ---------------------------------------------------------------------------


class TestReconciliationServiceMissingInventory:
    def test_returns_missing_status_when_no_inventory_record(self) -> None:
        factory = make_session_factory()
        service = ReconciliationService(session_factory=factory)

        result = service.reconcile(store_id="STORE-A", product_id="SKU-999")

        assert result.status == ReconciliationStatus.MISSING
        assert result.expected_quantity == 0
        assert result.actual_quantity == 0
        assert result.difference == 0

    def test_persists_record_for_missing_inventory(self) -> None:
        factory = make_session_factory()
        service = ReconciliationService(session_factory=factory)

        service.reconcile(store_id="STORE-A", product_id="SKU-999")

        with factory() as session:
            records = session.execute(
                select(ReconciliationRecord)
                .where(ReconciliationRecord.store_id == "STORE-A")
                .where(ReconciliationRecord.product_id == "SKU-999")
            ).scalars().all()

        assert len(records) == 1
        assert records[0].status == ReconciliationStatus.MISSING


class TestReconciliationServiceMatch:
    def test_returns_match_when_actual_equals_expected(self) -> None:
        factory = make_session_factory()

        with factory() as session:
            with session.begin():
                inv = seed_inventory(
                    session, sku="SKU-100", warehouse_id="STORE-NYC", quantity=7
                )
                seed_history(
                    session,
                    inventory_id=inv.id,
                    delta=10,
                    quantity_before=0,
                    quantity_after=10,
                    change_type=InventoryChangeType.RESTOCK,
                )
                seed_history(
                    session,
                    inventory_id=inv.id,
                    delta=-3,
                    quantity_before=10,
                    quantity_after=7,
                    change_type=InventoryChangeType.SALE,
                )

        service = ReconciliationService(session_factory=factory)
        result = service.reconcile(store_id="STORE-NYC", product_id="SKU-100")

        assert result.status == ReconciliationStatus.MATCH
        assert result.expected_quantity == 7
        assert result.actual_quantity == 7
        assert result.difference == 0

    def test_persists_record_on_first_match(self) -> None:
        factory = make_session_factory()

        with factory() as session:
            with session.begin():
                inv = seed_inventory(
                    session, sku="SKU-100", warehouse_id="STORE-NYC", quantity=5
                )
                seed_history(
                    session,
                    inventory_id=inv.id,
                    delta=5,
                    quantity_before=0,
                    quantity_after=5,
                )

        service = ReconciliationService(session_factory=factory)
        service.reconcile(store_id="STORE-NYC", product_id="SKU-100")

        with factory() as session:
            records = session.execute(
                select(ReconciliationRecord)
                .where(ReconciliationRecord.store_id == "STORE-NYC")
                .where(ReconciliationRecord.product_id == "SKU-100")
            ).scalars().all()

        assert len(records) == 1
        assert records[0].status == ReconciliationStatus.MATCH


class TestReconciliationServiceMismatch:
    def test_returns_mismatch_when_actual_differs_from_expected(self) -> None:
        factory = make_session_factory()

        with factory() as session:
            with session.begin():
                inv = seed_inventory(
                    session, sku="SKU-200", warehouse_id="STORE-B", quantity=8
                )
                # history sums to 10 but actual is 8 → drift of -2
                seed_history(
                    session,
                    inventory_id=inv.id,
                    delta=10,
                    quantity_before=0,
                    quantity_after=10,
                )

        service = ReconciliationService(session_factory=factory)
        result = service.reconcile(store_id="STORE-B", product_id="SKU-200")

        assert result.status == ReconciliationStatus.MISMATCH
        assert result.expected_quantity == 10
        assert result.actual_quantity == 8
        assert result.difference == -2


class TestReconciliationServiceIdempotency:
    def test_does_not_persist_duplicate_record_when_state_unchanged(self) -> None:
        factory = make_session_factory()

        with factory() as session:
            with session.begin():
                inv = seed_inventory(
                    session, sku="SKU-300", warehouse_id="STORE-C", quantity=5
                )
                seed_history(
                    session,
                    inventory_id=inv.id,
                    delta=5,
                    quantity_before=0,
                    quantity_after=5,
                )

        service = ReconciliationService(session_factory=factory)
        first = service.reconcile(store_id="STORE-C", product_id="SKU-300")
        second = service.reconcile(store_id="STORE-C", product_id="SKU-300")

        # Both runs should report the same reconciled_at (from the original record)
        # Note: SQLite may not preserve timezone info, so we compare without tz
        assert first.reconciled_at.replace(tzinfo=None) == second.reconciled_at.replace(tzinfo=None)

        with factory() as session:
            count = len(
                session.execute(
                    select(ReconciliationRecord)
                    .where(ReconciliationRecord.store_id == "STORE-C")
                    .where(ReconciliationRecord.product_id == "SKU-300")
                ).scalars().all()
            )
        assert count == 1

    def test_persists_new_record_when_status_transitions(self) -> None:
        factory = make_session_factory()

        with factory() as session:
            with session.begin():
                inv = seed_inventory(
                    session, sku="SKU-400", warehouse_id="STORE-D", quantity=8
                )
                # Deliberately mismatched to start
                seed_history(
                    session,
                    inventory_id=inv.id,
                    delta=10,
                    quantity_before=0,
                    quantity_after=10,
                )

        service = ReconciliationService(session_factory=factory)
        first = service.reconcile(store_id="STORE-D", product_id="SKU-400")
        assert first.status == ReconciliationStatus.MISMATCH

        # Now fix the inventory to match history
        with factory() as session:
            with session.begin():
                inv = session.execute(
                    select(Inventory)
                    .where(Inventory.sku == "SKU-400")
                    .where(Inventory.warehouse_id == "STORE-D")
                ).scalar_one()
                inv.quantity = 10

        second = service.reconcile(store_id="STORE-D", product_id="SKU-400")
        assert second.status == ReconciliationStatus.MATCH

        with factory() as session:
            count = len(
                session.execute(
                    select(ReconciliationRecord)
                    .where(ReconciliationRecord.store_id == "STORE-D")
                    .where(ReconciliationRecord.product_id == "SKU-400")
                ).scalars().all()
            )
        assert count == 2

    def test_persists_new_record_when_difference_changes(self) -> None:
        """Same status (MISMATCH) but different magnitude triggers a new record."""
        factory = make_session_factory()

        with factory() as session:
            with session.begin():
                inv = seed_inventory(
                    session, sku="SKU-500", warehouse_id="STORE-E", quantity=8
                )
                seed_history(
                    session,
                    inventory_id=inv.id,
                    delta=10,
                    quantity_before=0,
                    quantity_after=10,
                )

        service = ReconciliationService(session_factory=factory)
        service.reconcile(store_id="STORE-E", product_id="SKU-500")

        # Increase the drift by selling more without a history entry
        with factory() as session:
            with session.begin():
                inv = session.execute(
                    select(Inventory)
                    .where(Inventory.sku == "SKU-500")
                    .where(Inventory.warehouse_id == "STORE-E")
                ).scalar_one()
                inv.quantity = 6

        service.reconcile(store_id="STORE-E", product_id="SKU-500")

        with factory() as session:
            records = session.execute(
                select(ReconciliationRecord)
                .where(ReconciliationRecord.store_id == "STORE-E")
                .where(ReconciliationRecord.product_id == "SKU-500")
                .order_by(ReconciliationRecord.reconciled_at)
            ).scalars().all()

        assert len(records) == 2
        assert records[0].difference == -2
        assert records[1].difference == -4


class TestReconciliationServiceEmptyHistory:
    def test_inventory_with_no_history_and_zero_quantity_is_match(self) -> None:
        factory = make_session_factory()

        with factory() as session:
            with session.begin():
                seed_inventory(
                    session, sku="SKU-ZERO", warehouse_id="STORE-F", quantity=0
                )

        service = ReconciliationService(session_factory=factory)
        result = service.reconcile(store_id="STORE-F", product_id="SKU-ZERO")

        assert result.status == ReconciliationStatus.MATCH
        assert result.expected_quantity == 0
        assert result.actual_quantity == 0

    def test_inventory_with_no_history_but_nonzero_quantity_is_mismatch(self) -> None:
        factory = make_session_factory()

        with factory() as session:
            with session.begin():
                seed_inventory(
                    session, sku="SKU-GHOST", warehouse_id="STORE-G", quantity=5
                )

        service = ReconciliationService(session_factory=factory)
        result = service.reconcile(store_id="STORE-G", product_id="SKU-GHOST")

        assert result.status == ReconciliationStatus.MISMATCH
        assert result.expected_quantity == 0
        assert result.actual_quantity == 5
        assert result.difference == 5


class TestReconciliationServiceErrorHandling:
    def test_raises_transient_error_on_operational_failure(self) -> None:
        from unittest.mock import MagicMock, patch
        from sqlalchemy.exc import OperationalError

        factory = make_session_factory()
        service = ReconciliationService(session_factory=factory)

        with patch.object(
            service,
            "_reconcile_transactionally",
            side_effect=OperationalError("conn", {}, Exception("db down")),
        ):
            with pytest.raises(ReconciliationTransientError):
                service.reconcile(store_id="STORE-X", product_id="SKU-X")
