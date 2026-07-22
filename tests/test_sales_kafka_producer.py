from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from kafka.errors import KafkaTimeoutError

from app.producer.sales_kafka_producer import KafkaSalesPublishError, KafkaSalesProducer
from app.schemas.sales import SalesEvent


def make_event(**kwargs) -> SalesEvent:
    defaults = dict(
        event_id=uuid4(),
        sale_id="ORDER-001",
        product_id="SKU-100",
        store_id="STORE-A",
        quantity_sold=5,
        sale_price=Decimal("29.99"),
        timestamp=datetime.now(UTC),
    )
    defaults.update(kwargs)
    return SalesEvent(**defaults)


def make_producer(
    mock_kafka_producer: MagicMock,
    *,
    attempts: int = 3,
    backoff: float = 0.0,
    timeout: int = 10,
    topic: str = "sales_events",
) -> KafkaSalesProducer:
    return KafkaSalesProducer(
        producer=mock_kafka_producer,
        topic=topic,
        publish_attempts=attempts,
        publish_retry_backoff_seconds=backoff,
        publish_timeout_seconds=timeout,
    )


# ---------------------------------------------------------------------------
# Successful publish
# ---------------------------------------------------------------------------


def test_publish_sales_event_sends_to_correct_topic() -> None:
    kafka = MagicMock()
    future = MagicMock()
    future.get.return_value = MagicMock(partition=0, offset=10)
    kafka.send.return_value = future

    producer = make_producer(kafka)
    event = make_event()

    producer.publish_sales_event(event)

    kafka.send.assert_called_once()
    call_kwargs = kafka.send.call_args
    assert call_kwargs[0][0] == "sales_events"


def test_publish_sales_event_uses_event_id_as_key() -> None:
    kafka = MagicMock()
    future = MagicMock()
    future.get.return_value = MagicMock(partition=0, offset=1)
    kafka.send.return_value = future

    producer = make_producer(kafka)
    event = make_event()

    producer.publish_sales_event(event)

    key_used = kafka.send.call_args[1]["key"]
    assert key_used == str(event.event_id).encode("utf-8")


def test_publish_sales_event_succeeds_on_first_attempt() -> None:
    kafka = MagicMock()
    future = MagicMock()
    future.get.return_value = MagicMock(partition=0, offset=5)
    kafka.send.return_value = future

    producer = make_producer(kafka, attempts=3)
    producer.publish_sales_event(make_event())

    assert kafka.send.call_count == 1


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------


def test_publish_sales_event_retries_on_kafka_error_then_succeeds() -> None:
    kafka = MagicMock()
    failure_future = MagicMock()
    failure_future.get.side_effect = KafkaTimeoutError()
    success_future = MagicMock()
    success_future.get.return_value = MagicMock(partition=0, offset=2)
    kafka.send.side_effect = [failure_future, success_future]

    # Patch get to fail first call only
    kafka.send.side_effect = None
    kafka.send.return_value = failure_future
    failure_future.get.side_effect = [KafkaTimeoutError(), MagicMock(partition=0, offset=2)]

    producer = make_producer(kafka, attempts=3, backoff=0.0)
    producer.publish_sales_event(make_event())  # should not raise


def test_publish_sales_event_raises_after_all_attempts_exhausted() -> None:
    kafka = MagicMock()
    future = MagicMock()
    future.get.side_effect = KafkaTimeoutError()
    kafka.send.return_value = future

    producer = make_producer(kafka, attempts=3, backoff=0.0)

    with pytest.raises(KafkaSalesPublishError):
        producer.publish_sales_event(make_event())

    assert kafka.send.call_count == 3


def test_publish_sales_event_does_not_retry_on_success() -> None:
    kafka = MagicMock()
    future = MagicMock()
    future.get.return_value = MagicMock(partition=1, offset=99)
    kafka.send.return_value = future

    producer = make_producer(kafka, attempts=5, backoff=0.0)
    producer.publish_sales_event(make_event())

    assert kafka.send.call_count == 1


# ---------------------------------------------------------------------------
# Payload serialisation
# ---------------------------------------------------------------------------


def test_publish_sales_event_payload_contains_required_fields() -> None:
    kafka = MagicMock()
    future = MagicMock()
    future.get.return_value = MagicMock(partition=0, offset=0)
    kafka.send.return_value = future

    producer = make_producer(kafka)
    event = make_event(sale_id="ORDER-XYZ", product_id="SKU-A", store_id="NYC")
    producer.publish_sales_event(event)

    sent_value = kafka.send.call_args[1]["value"]
    assert sent_value["sale_id"] == "ORDER-XYZ"
    assert sent_value["product_id"] == "SKU-A"
    assert sent_value["store_id"] == "NYC"
    assert sent_value["quantity_sold"] == 5
