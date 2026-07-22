from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.database.models import Inventory, InventoryChangeType, InventoryHistory, Sales
from app.schemas.sales import SalesEvent, SalesSummaryResponse, SalesRecord

logger = logging.getLogger(__name__)


class SalesServiceError(Exception):
    """Base exception for sales service failures."""


class SalesTransientError(SalesServiceError):
    """Raised for transient failures that can be retried safely."""


class SalesBusinessRuleError(SalesServiceError):
    """Raised for non-retryable domain validation failures."""


class SalesInsufficientStockError(SalesBusinessRuleError):
    """Raised when there is not enough stock to fulfil the sale."""


class SalesProductNotFoundError(SalesBusinessRuleError):
    """Raised when the requested product/store combination does not exist."""


@dataclass(frozen=True)
class SalesProcessingResult:
    sale_id: str
    product_id: str
    store_id: str
    quantity_sold: int
    sale_price: Decimal | None
    duplicate: bool
    sales_record_id: UUID


class SalesService:
    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def process_event(self, event: SalesEvent) -> SalesProcessingResult:
        """Persist a sales transaction, deducting inventory in the same transaction.

        Idempotent: repeated calls with the same ``sale_id`` return the
        original result without modifying state.
        """
        try:
            return self._process_event_transactionally(event)
        except SalesBusinessRuleError:
            raise
        except OperationalError as exc:
            logger.exception(
                "sales_service_operational_error",
                extra={"event_id": str(event.event_id), "sale_id": event.sale_id},
            )
            raise SalesTransientError("database operational error") from exc
        except SQLAlchemyError as exc:
            logger.exception(
                "sales_service_database_error",
                extra={"event_id": str(event.event_id), "sale_id": event.sale_id},
            )
            raise SalesTransientError("database error") from exc

    def _process_event_transactionally(self, event: SalesEvent) -> SalesProcessingResult:
        with self._session_factory() as session:
            with session.begin():
                # --- idempotency check ----------------------------------------
                existing_sale = session.execute(
                    select(Sales).where(Sales.external_sale_id == event.sale_id)
                ).scalar_one_or_none()

                if existing_sale is not None:
                    logger.info(
                        "sales_event_duplicate_skipped",
                        extra={"sale_id": event.sale_id, "event_id": str(event.event_id)},
                    )
                    return SalesProcessingResult(
                        sale_id=event.sale_id,
                        product_id=event.product_id,
                        store_id=event.store_id,
                        quantity_sold=existing_sale.quantity_sold,
                        sale_price=(
                            Decimal(str(existing_sale.sale_price))
                            if existing_sale.sale_price is not None
                            else None
                        ),
                        duplicate=True,
                        sales_record_id=existing_sale.id,
                    )

                # --- resolve inventory (SELECT … FOR UPDATE) ------------------
                inventory = session.execute(
                    select(Inventory)
                    .where(Inventory.sku == event.product_id)
                    .where(Inventory.warehouse_id == event.store_id)
                    .with_for_update()
                ).scalar_one_or_none()

                if inventory is None:
                    raise SalesProductNotFoundError(
                        f"no inventory record for product_id={event.product_id!r} "
                        f"store_id={event.store_id!r}"
                    )

                if inventory.quantity < event.quantity_sold:
                    raise SalesInsufficientStockError(
                        f"insufficient stock: available={inventory.quantity}, "
                        f"requested={event.quantity_sold}"
                    )

                quantity_before = inventory.quantity
                quantity_after = quantity_before - event.quantity_sold

                # --- persist sale record --------------------------------------
                sale = Sales(
                    inventory_id=inventory.id,
                    quantity_sold=event.quantity_sold,
                    sale_price=event.sale_price,
                    external_sale_id=event.sale_id,
                    sold_at=event.timestamp,
                )
                session.add(sale)

                # --- deduct inventory -----------------------------------------
                inventory.quantity = quantity_after
                session.add(
                    InventoryHistory(
                        inventory_id=inventory.id,
                        change_type=InventoryChangeType.SALE,
                        quantity_before=quantity_before,
                        quantity_after=quantity_after,
                        quantity_delta=-event.quantity_sold,
                        source_event_id=str(event.event_id),
                    )
                )

                session.flush()

                logger.info(
                    "sales_event_processed",
                    extra={
                        "sale_id": event.sale_id,
                        "event_id": str(event.event_id),
                        "product_id": event.product_id,
                        "store_id": event.store_id,
                        "quantity_sold": event.quantity_sold,
                        "quantity_before": quantity_before,
                        "quantity_after": quantity_after,
                    },
                )

                return SalesProcessingResult(
                    sale_id=event.sale_id,
                    product_id=event.product_id,
                    store_id=event.store_id,
                    quantity_sold=event.quantity_sold,
                    sale_price=event.sale_price,
                    duplicate=False,
                    sales_record_id=sale.id,
                )

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def get_sales_summary(
        self,
        session: Session,
        product_id: str,
        store_id: str,
    ) -> SalesSummaryResponse | None:
        """Return a sales summary for a product/store pair.

        Returns ``None`` when no inventory record exists for the combination.
        """
        inventory = session.execute(
            select(Inventory)
            .where(Inventory.sku == product_id)
            .where(Inventory.warehouse_id == store_id)
        ).scalar_one_or_none()

        if inventory is None:
            return None

        sales_rows = (
            session.execute(
                select(Sales)
                .where(Sales.inventory_id == inventory.id)
                .order_by(Sales.sold_at.desc())
            )
            .scalars()
            .all()
        )

        total_quantity_sold = sum(s.quantity_sold for s in sales_rows)
        transaction_count = len(sales_rows)

        priced = [
            Decimal(str(s.sale_price)) * s.quantity_sold
            for s in sales_rows
            if s.sale_price is not None
        ]
        total_revenue: Decimal | None = sum(priced) if priced else None

        return SalesSummaryResponse(
            product_id=product_id,
            store_id=store_id,
            total_quantity_sold=total_quantity_sold,
            transaction_count=transaction_count,
            total_revenue=total_revenue,
            sales=[SalesRecord.model_validate(s) for s in sales_rows],
        )


def get_sales_service() -> SalesService:
    from app.database.session import SessionLocal

    return SalesService(session_factory=SessionLocal)
