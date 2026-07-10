from datetime import datetime

import pytest
from pydantic import ValidationError

from app.schemas.inventory import InventoryEventCreate


def test_inventory_event_create_validation_rejects_invalid_operation() -> None:
    with pytest.raises(ValidationError):
        InventoryEventCreate(
            product_id="SKU-100",
            store_id="STORE-01",
            operation="invalid-operation",
            quantity=2,
            timestamp="2026-07-09T10:20:00Z",
        )


def test_inventory_event_create_validation_rejects_naive_timestamp() -> None:
    with pytest.raises(ValidationError):
        InventoryEventCreate(
            product_id="SKU-101",
            store_id="STORE-02",
            operation="RESTOCK",
            quantity=4,
            timestamp=datetime(2026, 7, 9, 10, 20, 0),
        )


def test_inventory_event_create_validation_rejects_zero_manual_adjustment() -> None:
    with pytest.raises(ValidationError):
        InventoryEventCreate(
            product_id="SKU-102",
            store_id="STORE-03",
            operation="MANUAL_ADJUSTMENT",
            quantity=0,
            timestamp="2026-07-09T10:20:00Z",
        )


def test_inventory_event_create_validation_rejects_non_positive_sale_quantity() -> None:
    with pytest.raises(ValidationError):
        InventoryEventCreate(
            product_id="SKU-103",
            store_id="STORE-04",
            operation="SALE",
            quantity=0,
            timestamp="2026-07-09T10:20:00Z",
        )
