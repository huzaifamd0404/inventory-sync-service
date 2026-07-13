from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.database.base import Base
from app.database.models import Inventory, InventoryChangeType, InventoryHistory
from app.schemas.inventory import InventoryEvent, InventoryOperation
from app.services.inventory_service import (
    InventoryBusinessRuleError,
    InventoryService,
    InventoryTransientError,
)


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def set(self, key: str, value: str) -> bool:
        self.values[key] = value
        return True


class FailingRedis:
    def set(self, _: str, __: str) -> bool:
        raise RedisConnectionError("redis unavailable")


def make_event(event_id: str, operation: InventoryOperation, quantity: int) -> InventoryEvent:
    return InventoryEvent(
        event_id=UUID(event_id),
        product_id="SKU-100",
        store_id="STORE-A",
        operation=operation,
        quantity=quantity,
        timestamp=datetime.now(UTC),
    )


def make_session_factory() -> sessionmaker[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_inventory_service_processes_all_supported_operations_and_records_history() -> None:
    session_factory = make_session_factory()
    redis_client = FakeRedis()
    service = InventoryService(session_factory=session_factory, redis_client=redis_client)

    restock = service.process_event(
        make_event("5f16b7e1-2e31-4da8-bce4-b9fd5f52b8ef", InventoryOperation.RESTOCK, 10)
    )
    sale = service.process_event(
        make_event("9f7868ee-a8fd-44f9-9cae-4f4e2557ef50", InventoryOperation.SALE, 3)
    )
    returned = service.process_event(
        make_event("f973ec2d-5bd5-421f-8d1d-cf6677717c78", InventoryOperation.RETURN, 2)
    )
    adjusted = service.process_event(
        make_event(
            "f1ca6cb1-b90c-45b2-b617-0f1f7f851c69",
            InventoryOperation.MANUAL_ADJUSTMENT,
            -1,
        )
    )

    assert restock.quantity_after == 10
    assert sale.quantity_after == 7
    assert returned.quantity_after == 9
    assert adjusted.quantity_after == 8

    with session_factory() as session:
        inventory = session.execute(
            select(Inventory)
            .where(Inventory.sku == "SKU-100")
            .where(Inventory.warehouse_id == "STORE-A")
        ).scalar_one()
        history_entries = (
            session.execute(
                select(InventoryHistory).where(InventoryHistory.inventory_id == inventory.id)
            )
            .scalars()
            .all()
        )

    assert inventory.quantity == 8
    assert len(history_entries) == 4
    assert [entry.change_type for entry in history_entries] == [
        InventoryChangeType.RESTOCK,
        InventoryChangeType.SALE,
        InventoryChangeType.RETURN,
        InventoryChangeType.ADJUSTMENT,
    ]
    assert "inventory:STORE-A:SKU-100" in redis_client.values


def test_inventory_service_rejects_negative_resulting_quantity() -> None:
    session_factory = make_session_factory()
    service = InventoryService(session_factory=session_factory, redis_client=FakeRedis())

    with pytest.raises(InventoryBusinessRuleError):
        service.process_event(
            make_event("5004b7f4-4660-4d90-a775-b4be5f17244c", InventoryOperation.SALE, 1)
        )


def test_inventory_service_is_idempotent_for_duplicate_event_id() -> None:
    session_factory = make_session_factory()
    service = InventoryService(session_factory=session_factory, redis_client=FakeRedis())

    event = make_event("808cb4cd-d970-4ca3-861a-c8aa0fc18fdf", InventoryOperation.RESTOCK, 6)
    first = service.process_event(event)
    second = service.process_event(event)

    assert first.duplicate is False
    assert second.duplicate is True

    with session_factory() as session:
        history_count = session.execute(select(InventoryHistory)).scalars().all()

    assert len(history_count) == 1


def test_inventory_service_raises_transient_error_when_redis_sync_fails_after_commit() -> None:
    session_factory = make_session_factory()
    service = InventoryService(session_factory=session_factory, redis_client=FailingRedis())

    with pytest.raises(InventoryTransientError):
        service.process_event(
            make_event("96b73e4f-7c2f-4bb9-97bc-f123a21d4375", InventoryOperation.RESTOCK, 4)
        )

    with session_factory() as session:
        inventory = session.execute(select(Inventory)).scalar_one()
        history = session.execute(select(InventoryHistory)).scalars().all()

    assert inventory.quantity == 4
    assert len(history) == 1
