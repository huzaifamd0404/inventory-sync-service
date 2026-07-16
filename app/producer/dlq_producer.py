from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from kafka import KafkaProducer
from kafka.errors import KafkaError

from app.config.settings import Settings, get_settings

logger = logging.getLogger(__name__)


class DeadLetterQueuePublisher(Protocol):
    def publish_failed_event(
        self,
        *,
        event_id: str | None,
        source_topic: str,
        source_partition: int,
        source_offset: int,
        payload: object,
        failure_reason: str,
        retry_count: int,
    ) -> None: ...


class KafkaDlqPublishError(Exception):
    """Raised when publishing to the DLQ fails after all retries."""


class KafkaDlqProducer(DeadLetterQueuePublisher):
    def __init__(
        self,
        producer: KafkaProducer,
        topic: str,
        publish_attempts: int,
        publish_retry_backoff_seconds: float,
        publish_timeout_seconds: int,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._publish_attempts = publish_attempts
        self._publish_retry_backoff_seconds = publish_retry_backoff_seconds
        self._publish_timeout_seconds = publish_timeout_seconds

    @classmethod
    def from_settings(
        cls,
        settings: Settings | None = None,
        producer_factory: Callable[..., KafkaProducer] = KafkaProducer,
    ) -> "KafkaDlqProducer":
        configured_settings = settings or get_settings()
        producer = producer_factory(
            bootstrap_servers=configured_settings.kafka_bootstrap_servers,
            client_id=f"{configured_settings.kafka_client_id}-dlq",
            acks="all",
            retries=configured_settings.kafka_producer_retries,
            retry_backoff_ms=configured_settings.kafka_producer_retry_backoff_ms,
            linger_ms=configured_settings.kafka_producer_linger_ms,
            request_timeout_ms=configured_settings.kafka_producer_request_timeout_ms,
            delivery_timeout_ms=configured_settings.kafka_producer_delivery_timeout_ms,
            max_block_ms=configured_settings.kafka_producer_max_block_ms,
            value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode(
                "utf-8"
            ),
        )
        return cls(
            producer=producer,
            topic=configured_settings.kafka_topic_inventory_dlq,
            publish_attempts=configured_settings.kafka_publish_attempts,
            publish_retry_backoff_seconds=configured_settings.kafka_publish_retry_backoff_seconds,
            publish_timeout_seconds=configured_settings.kafka_publish_timeout_seconds,
        )

    def publish_failed_event(
        self,
        *,
        event_id: str | None,
        source_topic: str,
        source_partition: int,
        source_offset: int,
        payload: object,
        failure_reason: str,
        retry_count: int,
    ) -> None:
        message = {
            "event_id": event_id,
            "failure_reason": failure_reason,
            "retry_count": retry_count,
            "timestamp": datetime.now(UTC).isoformat(),
            "source_topic": source_topic,
            "source_partition": source_partition,
            "source_offset": source_offset,
            "payload": payload,
        }

        for attempt in range(1, self._publish_attempts + 1):
            try:
                key = (event_id or f"{source_topic}:{source_partition}:{source_offset}").encode("utf-8")
                future: Any = self._producer.send(self._topic, value=message, key=key)
                future.get(timeout=self._publish_timeout_seconds)
                logger.info(
                    "dlq_event_published",
                    extra={
                        "event_id": event_id,
                        "topic": self._topic,
                        "attempt": attempt,
                        "source_topic": source_topic,
                        "source_partition": source_partition,
                        "source_offset": source_offset,
                        "retry_count": retry_count,
                    },
                )
                return
            except KafkaError as exc:
                logger.warning(
                    "dlq_event_publish_retry",
                    extra={
                        "event_id": event_id,
                        "topic": self._topic,
                        "attempt": attempt,
                        "max_attempts": self._publish_attempts,
                        "error": str(exc),
                    },
                )
                if attempt == self._publish_attempts:
                    logger.exception(
                        "dlq_event_publish_failed",
                        extra={
                            "event_id": event_id,
                            "topic": self._topic,
                            "source_topic": source_topic,
                            "source_partition": source_partition,
                            "source_offset": source_offset,
                        },
                    )
                    raise KafkaDlqPublishError("failed to publish event to dead letter queue") from exc
                time.sleep(self._publish_retry_backoff_seconds)
