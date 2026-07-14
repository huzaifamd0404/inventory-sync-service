from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from app.consumer.inventory_event_validator import InventoryEventValidator
from app.schemas.inventory import InventoryEvent, InventoryOperation
from app.services.exceptions import InvalidInventoryEvent


def make_event(
    *,
    operation: InventoryOperation = InventoryOperation.RESTOCK,
    quantity: int = 3,
    timestamp: datetime | None = None,
    product_id: str = "SKU-100",
    store_id: str = "STORE-1",
) -> InventoryEvent:
    return InventoryEvent(
        event_id=UUID("9e7fce4f-ccee-4b3c-9f8c-c4ce2d6efeb0"),
        product_id=product_id,
        store_id=store_id,
        operation=operation,
        quantity=quantity,
        timestamp=timestamp or datetime.now(UTC),
    )


def test_inventory_event_validator_accepts_valid_event() -> None:
    validator = InventoryEventValidator(max_future_skew_seconds=300)
    event = make_event()

    validated = validator.validate(event)

    assert validated == event


def test_inventory_event_validator_rejects_blank_identifiers() -> None:
    validator = InventoryEventValidator()
    blank_product_event = InventoryEvent.model_construct(
        event_id=UUID("9e7fce4f-ccee-4b3c-9f8c-c4ce2d6efeb0"),
        product_id="   ",
        store_id="STORE-1",
        operation=InventoryOperation.RESTOCK,
        quantity=3,
        timestamp=datetime.now(UTC),
    )
    blank_store_event = InventoryEvent.model_construct(
        event_id=UUID("9e7fce4f-ccee-4b3c-9f8c-c4ce2d6efeb0"),
        product_id="SKU-100",
        store_id="   ",
        operation=InventoryOperation.RESTOCK,
        quantity=3,
        timestamp=datetime.now(UTC),
    )

    with pytest.raises(InvalidInventoryEvent):
        validator.validate(blank_product_event)

    with pytest.raises(InvalidInventoryEvent):
        validator.validate(blank_store_event)


def test_inventory_event_validator_rejects_future_timestamp_outside_skew() -> None:
    validator = InventoryEventValidator(max_future_skew_seconds=30)
    future_event = make_event(timestamp=datetime.now(UTC) + timedelta(minutes=2))

    with pytest.raises(InvalidInventoryEvent):
        validator.validate(future_event)


def test_inventory_event_validator_rejects_invalid_quantities() -> None:
    validator = InventoryEventValidator()
    zero_sale = InventoryEvent.model_construct(
        event_id=UUID("9e7fce4f-ccee-4b3c-9f8c-c4ce2d6efeb0"),
        product_id="SKU-100",
        store_id="STORE-1",
        operation=InventoryOperation.SALE,
        quantity=0,
        timestamp=datetime.now(UTC),
    )
    zero_adjustment = InventoryEvent.model_construct(
        event_id=UUID("9e7fce4f-ccee-4b3c-9f8c-c4ce2d6efeb0"),
        product_id="SKU-100",
        store_id="STORE-1",
        operation=InventoryOperation.MANUAL_ADJUSTMENT,
        quantity=0,
        timestamp=datetime.now(UTC),
    )

    with pytest.raises(InvalidInventoryEvent):
        validator.validate(zero_sale)

    with pytest.raises(InvalidInventoryEvent):
        validator.validate(zero_adjustment)
