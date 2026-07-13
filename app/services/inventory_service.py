from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from redis import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.cache.redis_client import get_redis_client
from app.database.models import Inventory, InventoryChangeType, InventoryHistory
from app.database.session import SessionLocal
from app.schemas.inventory import InventoryEvent, InventoryOperation

logger = logging.getLogger(__name__)


class InventoryServiceError(Exception):
    """Base exception for inventory service failures."""


class InventoryTransientError(InventoryServiceError):
    """Raised for transient failures that can be retried safely."""


class InventoryBusinessRuleError(InventoryServiceError):
    """Raised for non-retryable domain validation failures."""


@dataclass(frozen=True)
class InventoryProcessingResult:
    event_id: str
    product_id: str
    store_id: str
    operation: str
    quantity_before: int
    quantity_after: int
    quantity_delta: int
    duplicate: bool


class InventoryService:
    def __init__(
        self,
        session_factory: Callable[[], Session],
        redis_client: Redis,
    ) -> None:
        self._session_factory = session_factory
        self._redis_client = redis_client

    def process_event(self, event: InventoryEvent) -> InventoryProcessingResult:
        try:
            result = self._process_event_transactionally(event)
        except InventoryBusinessRuleError:
            raise
        except OperationalError as exc:
            logger.exception(
                "inventory_service_operational_error",
                extra={"event_id": str(event.event_id)},
            )
            raise InventoryTransientError("database operational error") from exc
        except SQLAlchemyError as exc:
            logger.exception(
                "inventory_service_database_error",
                extra={"event_id": str(event.event_id)},
            )
            raise InventoryTransientError("database error") from exc

        self._sync_cache(result)
        return result

    def _process_event_transactionally(self, event: InventoryEvent) -> InventoryProcessingResult:
        source_event_id = str(event.event_id)

        with self._session_factory() as session:
            with session.begin():
                duplicate = session.execute(
                    select(InventoryHistory).where(
                        InventoryHistory.source_event_id == source_event_id
                    )
                ).scalar_one_or_none()
                if duplicate is not None:
                    inventory = session.get(Inventory, duplicate.inventory_id)
                    if inventory is None:
                        raise InventoryTransientError("duplicate history found without inventory")
                    return InventoryProcessingResult(
                        event_id=source_event_id,
                        product_id=event.product_id,
                        store_id=event.store_id,
                        operation=event.operation.value,
                        quantity_before=inventory.quantity,
                        quantity_after=inventory.quantity,
                        quantity_delta=0,
                        duplicate=True,
                    )

                inventory = session.execute(
                    select(Inventory)
                    .where(Inventory.sku == event.product_id)
                    .where(Inventory.warehouse_id == event.store_id)
                    .with_for_update()
                ).scalar_one_or_none()

                if inventory is None:
                    inventory = Inventory(
                        sku=event.product_id,
                        warehouse_id=event.store_id,
                        quantity=0,
                        reorder_level=0,
                        is_active=True,
                    )
                    session.add(inventory)
                    session.flush()

                quantity_before = inventory.quantity
                quantity_delta, change_type = self._resolve_change(event)
                quantity_after = quantity_before + quantity_delta

                if quantity_after < 0:
                    raise InventoryBusinessRuleError(
                        "inventory quantity cannot be negative after applying event"
                    )

                inventory.quantity = quantity_after
                session.add(
                    InventoryHistory(
                        inventory_id=inventory.id,
                        change_type=change_type,
                        quantity_before=quantity_before,
                        quantity_after=quantity_after,
                        quantity_delta=quantity_delta,
                        source_event_id=source_event_id,
                    )
                )

                return InventoryProcessingResult(
                    event_id=source_event_id,
                    product_id=event.product_id,
                    store_id=event.store_id,
                    operation=event.operation.value,
                    quantity_before=quantity_before,
                    quantity_after=quantity_after,
                    quantity_delta=quantity_delta,
                    duplicate=False,
                )

    def _resolve_change(self, event: InventoryEvent) -> tuple[int, InventoryChangeType]:
        if event.operation == InventoryOperation.SALE:
            return -event.quantity, InventoryChangeType.SALE
        if event.operation == InventoryOperation.RESTOCK:
            return event.quantity, InventoryChangeType.RESTOCK
        if event.operation == InventoryOperation.RETURN:
            return event.quantity, InventoryChangeType.RETURN
        if event.operation == InventoryOperation.MANUAL_ADJUSTMENT:
            return event.quantity, InventoryChangeType.ADJUSTMENT
        raise InventoryBusinessRuleError(f"unsupported operation: {event.operation.value}")

    def _sync_cache(self, result: InventoryProcessingResult) -> None:
        cache_key = f"inventory:{result.store_id}:{result.product_id}"
        payload = json.dumps(
            {
                "product_id": result.product_id,
                "store_id": result.store_id,
                "quantity": result.quantity_after,
                "operation": result.operation,
                "source_event_id": result.event_id,
                "synced_at": datetime.now(UTC).isoformat(),
            },
            separators=(",", ":"),
        )

        try:
            self._redis_client.set(cache_key, payload)
        except RedisError as exc:
            logger.exception(
                "inventory_service_redis_sync_failed",
                extra={
                    "event_id": result.event_id,
                    "product_id": result.product_id,
                    "store_id": result.store_id,
                },
            )
            raise InventoryTransientError("redis synchronization failed") from exc


def get_inventory_service() -> InventoryService:
    return InventoryService(session_factory=SessionLocal, redis_client=get_redis_client())
