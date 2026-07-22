from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.database.base import Base
from app.database.models import Inventory, InventoryHistory, Sales
from app.schemas.sales import SalesEvent
from app.services.sales_service import (
    SalesInsufficientStockError,
    SalesProductNotFoundError,
    SalesService,
    SalesTransientError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_session_factory() -> sessionmaker[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def make_event(
    *,
    sale_id: str = "ORDER-001",
    product_id: str = "SKU-100",
    store_id: str = "STORE-A",
    quantity_sold: int = 5,
    sale_price: Decimal | None = Decimal("19.99"),
    event_id: UUID | None = None,
) -> SalesEvent:
    return SalesEvent(
        event_id=event_id or uuid4(),
        sale_id=sale_id,
        product_id=product_id,
        store_id=store_id,
        quantity_sold=quantity_sold,
        sale_price=sale_price,
        timestamp=datetime.now(UTC),
    )


def seed_inventory(session_factory: sessionmaker[Session], quantity: int = 20) -> UUID:
    """Insert an Inventory row and return its id."""
    with session_factory() as session:
        with session.begin():
            inv = Inventory(
                sku="SKU-100",
                warehouse_id="STORE-A",
                quantity=quantity,
                reorder_level=0,
                is_active=True,
            )
            session.add(inv)
            session.flush()
            return inv.id


# ---------------------------------------------------------------------------
# process_event – happy path
# ---------------------------------------------------------------------------


def test_process_event_persists_sale_record() -> None:
    sf = make_session_factory()
    inv_id = seed_inventory(sf, quantity=10)
    service = SalesService(session_factory=sf)

    result = service.process_event(make_event(quantity_sold=3))

    assert result.duplicate is False
    assert result.quantity_sold == 3
    assert result.sale_price == Decimal("19.99")

    with sf() as session:
        sale = session.execute(
            select(Sales).where(Sales.inventory_id == inv_id)
        ).scalar_one()
        assert sale.quantity_sold == 3
        assert sale.external_sale_id == "ORDER-001"


def test_process_event_deducts_inventory() -> None:
    sf = make_session_factory()
    seed_inventory(sf, quantity=10)
    service = SalesService(session_factory=sf)

    service.process_event(make_event(quantity_sold=4))

    with sf() as session:
        inv = session.execute(
            select(Inventory)
            .where(Inventory.sku == "SKU-100")
            .where(Inventory.warehouse_id == "STORE-A")
        ).scalar_one()
        assert inv.quantity == 6


def test_process_event_records_inventory_history() -> None:
    sf = make_session_factory()
    inv_id = seed_inventory(sf, quantity=15)
    service = SalesService(session_factory=sf)

    service.process_event(make_event(quantity_sold=5))

    with sf() as session:
        history = session.execute(
            select(InventoryHistory).where(InventoryHistory.inventory_id == inv_id)
        ).scalar_one()
        assert history.quantity_before == 15
        assert history.quantity_after == 10
        assert history.quantity_delta == -5


def test_process_event_with_no_sale_price() -> None:
    sf = make_session_factory()
    seed_inventory(sf, quantity=10)
    service = SalesService(session_factory=sf)

    result = service.process_event(make_event(sale_price=None))

    assert result.sale_price is None
    with sf() as session:
        sale = session.execute(select(Sales)).scalar_one()
        assert sale.sale_price is None


# ---------------------------------------------------------------------------
# process_event – idempotency
# ---------------------------------------------------------------------------


def test_process_event_is_idempotent_for_same_sale_id() -> None:
    sf = make_session_factory()
    seed_inventory(sf, quantity=20)
    service = SalesService(session_factory=sf)

    event = make_event(quantity_sold=5)
    first = service.process_event(event)
    second = service.process_event(
        make_event(sale_id=event.sale_id, quantity_sold=99)  # different qty – ignored
    )

    assert first.duplicate is False
    assert second.duplicate is True
    assert second.quantity_sold == 5  # original value returned

    # Only one sale record and inventory deducted once
    with sf() as session:
        sales_count = len(session.execute(select(Sales)).scalars().all())
        inv = session.execute(select(Inventory)).scalar_one()
        assert sales_count == 1
        assert inv.quantity == 15


def test_process_event_idempotent_result_preserves_sales_record_id() -> None:
    sf = make_session_factory()
    seed_inventory(sf, quantity=20)
    service = SalesService(session_factory=sf)

    event = make_event()
    first = service.process_event(event)
    second = service.process_event(make_event(sale_id=event.sale_id))

    assert first.sales_record_id == second.sales_record_id


# ---------------------------------------------------------------------------
# process_event – error cases
# ---------------------------------------------------------------------------


def test_process_event_raises_insufficient_stock() -> None:
    sf = make_session_factory()
    seed_inventory(sf, quantity=2)
    service = SalesService(session_factory=sf)

    with pytest.raises(SalesInsufficientStockError):
        service.process_event(make_event(quantity_sold=10))


def test_process_event_raises_product_not_found() -> None:
    sf = make_session_factory()
    # No inventory seeded
    service = SalesService(session_factory=sf)

    with pytest.raises(SalesProductNotFoundError):
        service.process_event(make_event(product_id="UNKNOWN-SKU"))


def test_process_event_exact_stock_sells_out() -> None:
    sf = make_session_factory()
    seed_inventory(sf, quantity=5)
    service = SalesService(session_factory=sf)

    result = service.process_event(make_event(quantity_sold=5))

    assert result.duplicate is False
    with sf() as session:
        inv = session.execute(select(Inventory)).scalar_one()
        assert inv.quantity == 0


# ---------------------------------------------------------------------------
# get_sales_summary
# ---------------------------------------------------------------------------


def test_get_sales_summary_returns_none_for_unknown_product() -> None:
    sf = make_session_factory()
    service = SalesService(session_factory=sf)

    with sf() as session:
        result = service.get_sales_summary(session, "GHOST-SKU", "STORE-A")

    assert result is None


def test_get_sales_summary_returns_empty_for_product_with_no_sales() -> None:
    sf = make_session_factory()
    seed_inventory(sf, quantity=10)
    service = SalesService(session_factory=sf)

    with sf() as session:
        summary = service.get_sales_summary(session, "SKU-100", "STORE-A")

    assert summary is not None
    assert summary.transaction_count == 0
    assert summary.total_quantity_sold == 0
    assert summary.total_revenue is None
    assert summary.sales == []


def test_get_sales_summary_aggregates_multiple_transactions() -> None:
    sf = make_session_factory()
    seed_inventory(sf, quantity=50)
    service = SalesService(session_factory=sf)

    service.process_event(make_event(sale_id="S-1", quantity_sold=3, sale_price=Decimal("10.00")))
    service.process_event(make_event(sale_id="S-2", quantity_sold=2, sale_price=Decimal("15.00")))
    service.process_event(make_event(sale_id="S-3", quantity_sold=1, sale_price=None))

    with sf() as session:
        summary = service.get_sales_summary(session, "SKU-100", "STORE-A")

    assert summary is not None
    assert summary.transaction_count == 3
    assert summary.total_quantity_sold == 6
    # revenue = 3*10 + 2*15 = 30 + 30 = 60; sale S-3 has no price so excluded
    assert summary.total_revenue == Decimal("60.00")
    assert len(summary.sales) == 3


def test_get_sales_summary_total_revenue_is_none_when_all_prices_missing() -> None:
    sf = make_session_factory()
    seed_inventory(sf, quantity=20)
    service = SalesService(session_factory=sf)

    service.process_event(make_event(sale_id="S-1", quantity_sold=2, sale_price=None))

    with sf() as session:
        summary = service.get_sales_summary(session, "SKU-100", "STORE-A")

    assert summary is not None
    assert summary.total_revenue is None
