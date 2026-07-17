from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.database.models import (
    Inventory,
    InventoryHistory,
    ReconciliationRecord,
    ReconciliationStatus,
)

logger = logging.getLogger(__name__)


class ReconciliationError(Exception):
    """Base exception for reconciliation failures."""


class ReconciliationTransientError(ReconciliationError):
    """Raised for transient infrastructure failures that may be retried."""


@dataclass(frozen=True)
class ReconciliationResult:
    store_id: str
    product_id: str
    expected_quantity: int
    actual_quantity: int
    difference: int
    status: ReconciliationStatus
    reconciled_at: datetime


class ReconciliationService:
    """Calculates expected inventory from history, compares with actual state,
    and persists a new reconciliation record only when status or difference changes."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def reconcile(self, store_id: str, product_id: str) -> ReconciliationResult:
        """Run reconciliation for the given store/product pair.

        Queries the inventory history to derive the expected quantity, compares it
        against the current inventory snapshot, and persists a new record only when
        status or difference has changed since the last run.

        Args:
            store_id: Warehouse / store identifier (maps to ``Inventory.warehouse_id``).
            product_id: Product SKU (maps to ``Inventory.sku``).

        Returns:
            A :class:`ReconciliationResult` describing the current state.

        Raises:
            ReconciliationTransientError: When a transient database error occurs.
        """
        try:
            return self._reconcile_transactionally(store_id, product_id)
        except OperationalError as exc:
            logger.exception(
                "reconciliation_operational_error",
                extra={"store_id": store_id, "product_id": product_id},
            )
            raise ReconciliationTransientError("database operational error") from exc
        except SQLAlchemyError as exc:
            logger.exception(
                "reconciliation_database_error",
                extra={"store_id": store_id, "product_id": product_id},
            )
            raise ReconciliationTransientError("database error") from exc

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _reconcile_transactionally(
        self, store_id: str, product_id: str
    ) -> ReconciliationResult:
        with self._session_factory() as session:
            with session.begin():
                expected, actual, status = self._compute_reconciliation(
                    session, store_id, product_id
                )
                difference = actual - expected

                reconciled_at = self._persist_if_changed(
                    session,
                    store_id=store_id,
                    product_id=product_id,
                    expected_quantity=expected,
                    actual_quantity=actual,
                    difference=difference,
                    status=status,
                )

        logger.info(
            "reconciliation_completed",
            extra={
                "store_id": store_id,
                "product_id": product_id,
                "expected_quantity": expected,
                "actual_quantity": actual,
                "difference": difference,
                "status": status.value,
            },
        )

        return ReconciliationResult(
            store_id=store_id,
            product_id=product_id,
            expected_quantity=expected,
            actual_quantity=actual,
            difference=difference,
            status=status,
            reconciled_at=reconciled_at,
        )

    def _compute_reconciliation(
        self,
        session: Session,
        store_id: str,
        product_id: str,
    ) -> tuple[int, int, ReconciliationStatus]:
        """Return (expected_quantity, actual_quantity, status) for the given pair."""
        inventory = session.execute(
            select(Inventory)
            .where(Inventory.sku == product_id)
            .where(Inventory.warehouse_id == store_id)
        ).scalar_one_or_none()

        if inventory is None:
            logger.debug(
                "reconciliation_inventory_not_found",
                extra={"store_id": store_id, "product_id": product_id},
            )
            return 0, 0, ReconciliationStatus.MISSING

        delta_sum: int = session.execute(
            select(func.coalesce(func.sum(InventoryHistory.quantity_delta), 0)).where(
                InventoryHistory.inventory_id == inventory.id
            )
        ).scalar_one()

        expected = int(delta_sum)
        actual = inventory.quantity
        status = (
            ReconciliationStatus.MATCH
            if actual == expected
            else ReconciliationStatus.MISMATCH
        )
        return expected, actual, status

    def _persist_if_changed(
        self,
        session: Session,
        *,
        store_id: str,
        product_id: str,
        expected_quantity: int,
        actual_quantity: int,
        difference: int,
        status: ReconciliationStatus,
    ) -> datetime:
        """Persist a new reconciliation record only when state has changed.

        Returns the ``reconciled_at`` timestamp that callers should report.
        """
        latest: ReconciliationRecord | None = session.execute(
            select(ReconciliationRecord)
            .where(ReconciliationRecord.store_id == store_id)
            .where(ReconciliationRecord.product_id == product_id)
            .order_by(ReconciliationRecord.reconciled_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        if latest is not None and latest.status == status and latest.difference == difference:
            logger.debug(
                "reconciliation_no_state_change",
                extra={"store_id": store_id, "product_id": product_id},
            )
            return latest.reconciled_at

        now = datetime.now(UTC)
        record = ReconciliationRecord(
            store_id=store_id,
            product_id=product_id,
            expected_quantity=expected_quantity,
            actual_quantity=actual_quantity,
            difference=difference,
            status=status,
            reconciled_at=now,
        )
        session.add(record)
        logger.info(
            "reconciliation_record_persisted",
            extra={
                "store_id": store_id,
                "product_id": product_id,
                "status": status.value,
                "difference": difference,
            },
        )
        return now
