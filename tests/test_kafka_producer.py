from datetime import UTC, datetime
from uuid import UUID

import pytest
from kafka.errors import KafkaError

from app.config.settings import Settings
from app.producer.kafka_producer import KafkaInventoryProducer, KafkaPublishError
from app.schemas.inventory import InventoryEvent, InventoryOperation


class DummyMetadata:
    partition = 0
    offset = 42


class DummyFuture:
    def __init__(self, should_fail: bool) -> None:
        self._should_fail = should_fail

    def get(self, timeout: int) -> DummyMetadata:
        if self._should_fail:
            raise KafkaError("transient publish failure")
        return DummyMetadata()


class DummyKafkaProducer:
    def __init__(self, failures_before_success: int = 0) -> None:
        self.failures_before_success = failures_before_success
        self.send_calls = 0
        self.sent_topic: str | None = None
        self.sent_value: dict[str, object] | None = None

    def send(self, topic: str, value: dict[str, object], key: bytes) -> DummyFuture:
        self.send_calls += 1
        self.sent_topic = topic
        self.sent_value = value
        should_fail = self.send_calls <= self.failures_before_success
        return DummyFuture(should_fail=should_fail)


def test_from_settings_initializes_producer_with_expected_config() -> None:
    captured_kwargs: dict[str, object] = {}

    def fake_factory(**kwargs: object) -> DummyKafkaProducer:
        nonlocal captured_kwargs
        captured_kwargs = kwargs
        return DummyKafkaProducer()

    settings = Settings(
        kafka_bootstrap_servers="broker-a:9092,broker-b:9092",
        kafka_topic_inventory_updates="inventory_updates",
        kafka_client_id="inventory-sync-test",
        kafka_producer_retries=7,
        kafka_producer_retry_backoff_ms=100,
        kafka_producer_linger_ms=8,
        kafka_producer_request_timeout_ms=25000,
        kafka_producer_delivery_timeout_ms=70000,
        kafka_producer_max_block_ms=8000,
        kafka_publish_attempts=4,
        kafka_publish_retry_backoff_seconds=0,
        kafka_publish_timeout_seconds=3,
    )

    producer = KafkaInventoryProducer.from_settings(
        settings=settings, producer_factory=fake_factory
    )

    assert producer.topic == "inventory_updates"
    assert captured_kwargs["bootstrap_servers"] == "broker-a:9092,broker-b:9092"
    assert captured_kwargs["client_id"] == "inventory-sync-test"
    assert captured_kwargs["acks"] == "all"
    assert captured_kwargs["retries"] == 7


def test_publish_inventory_event_retries_then_succeeds() -> None:
    kafka_client = DummyKafkaProducer(failures_before_success=2)
    producer = KafkaInventoryProducer(
        producer=kafka_client,
        topic="inventory_updates",
        publish_attempts=3,
        publish_retry_backoff_seconds=0,
        publish_timeout_seconds=2,
    )

    event = InventoryEvent(
        event_id=UUID("0e9f4d70-98a3-41f3-b9bc-7439f4ac0f57"),
        product_id="SKU-1",
        store_id="STORE-1",
        operation=InventoryOperation.MANUAL_ADJUSTMENT,
        quantity=3,
        timestamp=datetime.now(UTC),
    )

    producer.publish_inventory_event(event)

    assert kafka_client.send_calls == 3
    assert kafka_client.sent_topic == "inventory_updates"
    assert kafka_client.sent_value is not None
    assert kafka_client.sent_value["event_id"] == "0e9f4d70-98a3-41f3-b9bc-7439f4ac0f57"


def test_publish_inventory_event_raises_after_retry_exhaustion() -> None:
    kafka_client = DummyKafkaProducer(failures_before_success=5)
    producer = KafkaInventoryProducer(
        producer=kafka_client,
        topic="inventory_updates",
        publish_attempts=2,
        publish_retry_backoff_seconds=0,
        publish_timeout_seconds=2,
    )

    event = InventoryEvent(
        event_id=UUID("ea33689f-a241-4df8-8f5d-74e6c292ec02"),
        product_id="SKU-2",
        store_id="STORE-9",
        operation=InventoryOperation.RESTOCK,
        quantity=1,
        timestamp=datetime.now(UTC),
    )

    with pytest.raises(KafkaPublishError):
        producer.publish_inventory_event(event)
