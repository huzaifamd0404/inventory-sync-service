from kafka.errors import KafkaError
import pytest

from app.producer.dlq_producer import KafkaDlqProducer, KafkaDlqPublishError


class DummyMetadata:
    partition = 1
    offset = 11


class DummyFuture:
    def __init__(self, should_fail: bool) -> None:
        self._should_fail = should_fail

    def get(self, timeout: int) -> DummyMetadata:
        if self._should_fail:
            raise KafkaError("send failed")
        return DummyMetadata()


class DummyKafkaProducer:
    def __init__(self, failures_before_success: int = 0) -> None:
        self.failures_before_success = failures_before_success
        self.send_calls = 0
        self.last_topic: str | None = None
        self.last_value: dict[str, object] | None = None

    def send(self, topic: str, value: dict[str, object], key: bytes) -> DummyFuture:
        self.send_calls += 1
        self.last_topic = topic
        self.last_value = value
        should_fail = self.send_calls <= self.failures_before_success
        return DummyFuture(should_fail=should_fail)


def test_dlq_producer_publishes_failed_event_with_required_metadata() -> None:
    producer_client = DummyKafkaProducer()
    producer = KafkaDlqProducer(
        producer=producer_client,
        topic="inventory_dlq",
        publish_attempts=2,
        publish_retry_backoff_seconds=0,
        publish_timeout_seconds=1,
    )

    producer.publish_failed_event(
        event_id="evt-123",
        source_topic="inventory_updates",
        source_partition=0,
        source_offset=7,
        payload={"event_id": "evt-123"},
        failure_reason="transient_failure_after_retries",
        retry_count=3,
    )

    assert producer_client.send_calls == 1
    assert producer_client.last_topic == "inventory_dlq"
    assert producer_client.last_value is not None
    assert producer_client.last_value["event_id"] == "evt-123"
    assert producer_client.last_value["failure_reason"] == "transient_failure_after_retries"
    assert producer_client.last_value["retry_count"] == 3
    assert "timestamp" in producer_client.last_value


def test_dlq_producer_raises_after_retry_exhaustion() -> None:
    producer_client = DummyKafkaProducer(failures_before_success=3)
    producer = KafkaDlqProducer(
        producer=producer_client,
        topic="inventory_dlq",
        publish_attempts=2,
        publish_retry_backoff_seconds=0,
        publish_timeout_seconds=1,
    )

    with pytest.raises(KafkaDlqPublishError):
        producer.publish_failed_event(
            event_id="evt-err",
            source_topic="inventory_updates",
            source_partition=0,
            source_offset=8,
            payload={"event_id": "evt-err"},
            failure_reason="non_retryable_failure",
            retry_count=0,
        )

    assert producer_client.send_calls == 2
