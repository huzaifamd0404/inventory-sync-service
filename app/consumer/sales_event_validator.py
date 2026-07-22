from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.schemas.sales import SalesEvent
from app.services.exceptions import InvalidInventoryEvent


class SalesEventValidator:
    def __init__(self, max_future_skew_seconds: int = 300) -> None:
        self._max_future_skew = timedelta(seconds=max_future_skew_seconds)

    def validate(self, event: SalesEvent) -> SalesEvent:
        if not event.product_id.strip():
            raise InvalidInventoryEvent("product_id must not be blank")

        if not event.store_id.strip():
            raise InvalidInventoryEvent("store_id must not be blank")

        if not event.sale_id.strip():
            raise InvalidInventoryEvent("sale_id must not be blank")

        if event.timestamp.tzinfo is None or event.timestamp.utcoffset() is None:
            raise InvalidInventoryEvent("timestamp must be timezone-aware")

        now = datetime.now(UTC)
        if event.timestamp > now + self._max_future_skew:
            raise InvalidInventoryEvent("timestamp is too far in the future")

        if event.quantity_sold <= 0:
            raise InvalidInventoryEvent("quantity_sold must be positive")

        return event
