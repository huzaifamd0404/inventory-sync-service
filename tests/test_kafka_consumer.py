from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

from app.consumer.kafka_consumer import KafkaInventoryConsumer
from app.database.base import Base
from app.database.models import Inventory, InventoryHistory
from app.schemas.inventory import InventoryEvent, InventoryOperation
from app.services.inventory_service import (
    InventoryBusinessRuleError,
    InventoryProcessingResult,
    InventoryService,
    InventoryTransientError,
)
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker


class DummyConsumer:
    def __init__(self) -> None:
        self.commit_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1


class StubInventoryService:
    def __init__(self, side_effects: list[Exception | InventoryProcessingResult]) -> None:
        self._side_effects = side_effects
        self.calls = 0

    def process_event(self, _: InventoryEvent) -> InventoryProcessingResult:
        self.calls += 1
        effect = self._side_effects[self.calls - 1]
        if isinstance(effect, Exception):
            raise effect
        return effect


def make_message(payload: dict[str, object]) -> SimpleNamespace:
    return SimpleNamespace(
        value=payload,
        topic="inventory_updates",
        partition=0,
        offset=5,
    )


def make_result() -> InventoryProcessingResult:
    return InventoryProcessingResult(
        event_id="2216a7d1-7f27-40b4-8453-2e8790f93f8f",
        product_id="SKU-77",
        store_id="STORE-4",
        operation="RESTOCK",
        quantity_before=10,
        quantity_after=15,
        quantity_delta=5,
        duplicate=False,
    )


def make_payload() -> dict[str, object]:
    return {
        "event_id": "2216a7d1-7f27-40b4-8453-2e8790f93f8f",
        "product_id": "SKU-77",
        "store_id": "STORE-4",
        "operation": "RESTOCK",
        "quantity": 5,
        "timestamp": datetime.now(UTC).isoformat(),
    }


def test_process_record_commits_after_successful_processing() -> None:
    consumer_client = DummyConsumer()
    service = StubInventoryService([make_result()])
    consumer = KafkaInventoryConsumer(
        consumer=consumer_client,
        inventory_service=service,
        max_attempts=3,
        retry_backoff_seconds=0,
    )

    consumer.process_record(make_message(make_payload()))

    assert service.calls == 1
    assert consumer_client.commit_calls == 1


def test_process_record_retries_transient_failure_then_commits() -> None:
    consumer_client = DummyConsumer()
    service = StubInventoryService(
        [InventoryTransientError("db timeout"), InventoryTransientError("redis timeout"), make_result()]
    )
    consumer = KafkaInventoryConsumer(
        consumer=consumer_client,
        inventory_service=service,
        max_attempts=3,
        retry_backoff_seconds=0,
    )

    consumer.process_record(make_message(make_payload()))

    assert service.calls == 3
    assert consumer_client.commit_calls == 1


def test_process_record_does_not_commit_when_transient_retries_exhausted() -> None:
    consumer_client = DummyConsumer()
    service = StubInventoryService(
        [
            InventoryTransientError("db timeout"),
            InventoryTransientError("db timeout"),
            InventoryTransientError("db timeout"),
        ]
    )
    consumer = KafkaInventoryConsumer(
        consumer=consumer_client,
        inventory_service=service,
        max_attempts=3,
        retry_backoff_seconds=0,
    )

    consumer.process_record(make_message(make_payload()))

    assert service.calls == 3
    assert consumer_client.commit_calls == 0


def test_process_record_commits_and_discards_non_retryable_event() -> None:
    consumer_client = DummyConsumer()
    service = StubInventoryService([InventoryBusinessRuleError("negative quantity")])
    consumer = KafkaInventoryConsumer(
        consumer=consumer_client,
        inventory_service=service,
        max_attempts=3,
        retry_backoff_seconds=0,
    )

    consumer.process_record(make_message(make_payload()))

    assert service.calls == 1
    assert consumer_client.commit_calls == 1


def test_process_record_commits_invalid_payload_to_skip_poison_messages() -> None:
    consumer_client = DummyConsumer()
    service = StubInventoryService([make_result()])
    consumer = KafkaInventoryConsumer(
        consumer=consumer_client,
        inventory_service=service,
        max_attempts=3,
        retry_backoff_seconds=0,
    )

    invalid_payload = {
        "event_id": str(UUID("2216a7d1-7f27-40b4-8453-2e8790f93f8f")),
        "product_id": "SKU-77",
        "store_id": "STORE-4",
        "operation": "RESTOCK",
        "quantity": 0,
        "timestamp": datetime.now(UTC).isoformat(),
    }

    consumer.process_record(make_message(invalid_payload))

    assert service.calls == 0
    assert consumer_client.commit_calls == 1


def test_process_record_integration_with_inventory_service_updates_db_and_commits() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

    class InMemoryRedis:
        def __init__(self) -> None:
            self.values: dict[str, str] = {}

        def set(self, key: str, value: str) -> bool:
            self.values[key] = value
            return True

    redis_client = InMemoryRedis()
    inventory_service = InventoryService(session_factory=session_factory, redis_client=redis_client)
    consumer_client = DummyConsumer()
    consumer = KafkaInventoryConsumer(
        consumer=consumer_client,
        inventory_service=inventory_service,
        max_attempts=2,
        retry_backoff_seconds=0,
    )

    payload = {
        "event_id": "6f8afab8-ac48-4eb8-a67b-e779db796f8d",
        "product_id": "SKU-500",
        "store_id": "STORE-Z",
        "operation": "RESTOCK",
        "quantity": 9,
        "timestamp": datetime.now(UTC).isoformat(),
    }

    consumer.process_record(make_message(payload))

    with session_factory() as session:
        inventory = session.execute(select(Inventory)).scalar_one()
        history = session.execute(select(InventoryHistory)).scalars().all()

    assert inventory.sku == "SKU-500"
    assert inventory.quantity == 9
    assert len(history) == 1
    assert consumer_client.commit_calls == 1
    assert "inventory:STORE-Z:SKU-500" in redis_client.values
