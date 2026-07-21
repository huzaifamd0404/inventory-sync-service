from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.consumer.kafka_consumer import KafkaInventoryConsumer
from app.consumer.inventory_event_validator import InventoryEventValidator
from app.database.base import Base
from app.database.models import Inventory, ProcessedEvent
from app.schemas.inventory import InventoryEvent
from app.services.failed_event_service import FailedEventService
from app.services.processed_event_service import ProcessedEventService
from app.services.inventory_service import InventoryService
from app.services.retry_service import RetryService


class DummyConsumer:
    def __init__(self) -> None:
        self.commit_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1

    def close(self) -> None:
        return None


class PassThroughValidator(InventoryEventValidator):
    def validate(self, event: InventoryEvent) -> InventoryEvent:
        return event


class StubDlqPublisher:
    def publish_failed_event(self, **_: object) -> None:
        return None


class InMemoryRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def set(self, key: str, value: str) -> bool:
        self.values[key] = value
        return True


def make_message(payload: dict[str, object], offset: int) -> SimpleNamespace:
    return SimpleNamespace(
        value=payload,
        topic="inventory_updates",
        partition=0,
        offset=offset,
    )


def make_payload(index: int) -> dict[str, object]:
    return {
        "event_id": str(uuid4()),
        "product_id": f"SKU-{index}",
        "store_id": "STORE-STRESS",
        "operation": "RESTOCK",
        "quantity": 3,
        "timestamp": datetime.now(UTC).isoformat(),
    }


def build_consumer(session_factory: sessionmaker) -> tuple[KafkaInventoryConsumer, DummyConsumer]:
    redis_client = InMemoryRedis()
    inventory_service = InventoryService(session_factory=session_factory, redis_client=redis_client)
    dummy_consumer = DummyConsumer()
    consumer = KafkaInventoryConsumer(
        consumer=dummy_consumer,
        inventory_service=inventory_service,
        processed_event_service=ProcessedEventService(session_factory=session_factory),
        failed_event_service=FailedEventService(session_factory=session_factory),
        dlq_publisher=StubDlqPublisher(),
        event_validator=PassThroughValidator(),
        retry_service=RetryService(
            max_attempts=2,
            initial_backoff_seconds=0,
            backoff_multiplier=2,
            max_backoff_seconds=0,
        ),
    )
    return consumer, dummy_consumer


def test_high_volume_processing_commits_all_events() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(
        bind=engine, autoflush=False, autocommit=False, expire_on_commit=False
    )

    consumer, dummy_consumer = build_consumer(session_factory)

    total_events = 1000
    for index in range(total_events):
        consumer.process_record(make_message(make_payload(index), offset=index))

    with session_factory() as session:
        inventory_count = session.execute(select(Inventory)).scalars().all()
        processed_count = session.execute(select(ProcessedEvent)).scalars().all()

    assert len(inventory_count) == total_events
    assert len(processed_count) == total_events
    assert dummy_consumer.commit_calls == total_events


def test_consumer_restart_keeps_idempotency() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(
        bind=engine, autoflush=False, autocommit=False, expire_on_commit=False
    )

    payload = {
        "event_id": str(uuid4()),
        "product_id": "SKU-RESTART",
        "store_id": "STORE-RESTART",
        "operation": "RESTOCK",
        "quantity": 7,
        "timestamp": datetime.now(UTC).isoformat(),
    }

    consumer_a, dummy_a = build_consumer(session_factory)
    consumer_a.process_record(make_message(payload, offset=1))

    # Simulate service restart by creating a fresh consumer instance.
    consumer_b, dummy_b = build_consumer(session_factory)
    consumer_b.process_record(make_message(payload, offset=2))

    with session_factory() as session:
        inventory = session.execute(select(Inventory)).scalar_one()
        processed_events = session.execute(select(ProcessedEvent)).scalars().all()

    assert inventory.quantity == 7
    assert len(processed_events) == 1
    assert dummy_a.commit_calls == 1
    assert dummy_b.commit_calls == 1
