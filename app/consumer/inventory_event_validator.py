from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.schemas.inventory import InventoryEvent, InventoryOperation
from app.services.exceptions import InvalidInventoryEvent


class InventoryEventValidator:
    def __init__(self, max_future_skew_seconds: int = 300) -> None:
        self._max_future_skew = timedelta(seconds=max_future_skew_seconds)

    def validate(self, event: InventoryEvent) -> InventoryEvent:
        if not event.product_id.strip():
            raise InvalidInventoryEvent("product_id must not be blank")

        if not event.store_id.strip():
            raise InvalidInventoryEvent("store_id must not be blank")

        if event.timestamp.tzinfo is None or event.timestamp.utcoffset() is None:
            raise InvalidInventoryEvent("timestamp must be timezone-aware")

        now = datetime.now(UTC)
        if event.timestamp > now + self._max_future_skew:
            raise InvalidInventoryEvent("timestamp is too far in the future")

        if event.operation in {
            InventoryOperation.SALE,
            InventoryOperation.RESTOCK,
            InventoryOperation.RETURN,
        } and event.quantity <= 0:
            raise InvalidInventoryEvent(f"quantity must be positive for {event.operation.value}")

        if event.operation == InventoryOperation.MANUAL_ADJUSTMENT and event.quantity == 0:
            raise InvalidInventoryEvent("quantity must be non-zero for MANUAL_ADJUSTMENT")

        return event
