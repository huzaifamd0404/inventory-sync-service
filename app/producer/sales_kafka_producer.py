from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any, Protocol

from kafka import KafkaProducer
from kafka.errors import KafkaError

from app.config.settings import Settings, get_settings
from app.schemas.sales import SalesEvent

logger = logging.getLogger(__name__)


class SalesEventPublisher(Protocol):
    def publish_sales_event(self, event: SalesEvent) -> None: ...


class KafkaSalesProducerInitializationError(Exception):
    """Raised when the Kafka sales producer cannot be initialised."""


class KafkaSalesPublishError(Exception):
    """Raised when publishing to Kafka fails after all retries."""


class KafkaSalesProducer(SalesEventPublisher):
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

    @property
    def topic(self) -> str:
        return self._topic

    @classmethod
    def from_settings(
        cls,
        settings: Settings | None = None,
        producer_factory: Callable[..., KafkaProducer] = KafkaProducer,
    ) -> "KafkaSalesProducer":
        configured_settings = settings or get_settings()

        try:
            producer = producer_factory(
                bootstrap_servers=configured_settings.kafka_bootstrap_servers,
                client_id=configured_settings.kafka_client_id,
                acks="all",
                retries=configured_settings.kafka_producer_retries,
                retry_backoff_ms=configured_settings.kafka_producer_retry_backoff_ms,
                linger_ms=configured_settings.kafka_producer_linger_ms,
                request_timeout_ms=configured_settings.kafka_producer_request_timeout_ms,
                delivery_timeout_ms=configured_settings.kafka_producer_delivery_timeout_ms,
                max_block_ms=configured_settings.kafka_producer_max_block_ms,
                value_serializer=lambda value: json.dumps(
                    value, separators=(",", ":"), default=str
                ).encode("utf-8"),
            )
        except KafkaError as exc:
            logger.exception("kafka_sales_producer_initialization_failed")
            raise KafkaSalesProducerInitializationError(
                "failed to initialize Kafka sales producer"
            ) from exc

        return cls(
            producer=producer,
            topic=configured_settings.kafka_topic_sales_events,
            publish_attempts=configured_settings.kafka_publish_attempts,
            publish_retry_backoff_seconds=configured_settings.kafka_publish_retry_backoff_seconds,
            publish_timeout_seconds=configured_settings.kafka_publish_timeout_seconds,
        )

    def publish_sales_event(self, event: SalesEvent) -> None:
        payload = event.model_dump(mode="json")

        for attempt in range(1, self._publish_attempts + 1):
            try:
                future: Any = self._producer.send(
                    self._topic,
                    value=payload,
                    key=str(event.event_id).encode("utf-8"),
                )
                record_metadata: Any = future.get(timeout=self._publish_timeout_seconds)
                logger.info(
                    "sales_event_published",
                    extra={
                        "event_id": str(event.event_id),
                        "sale_id": event.sale_id,
                        "product_id": event.product_id,
                        "store_id": event.store_id,
                        "topic": self._topic,
                        "partition": record_metadata.partition,
                        "offset": record_metadata.offset,
                        "attempt": attempt,
                    },
                )
                return
            except KafkaError as exc:
                logger.warning(
                    "sales_event_publish_attempt_failed",
                    extra={
                        "event_id": str(event.event_id),
                        "sale_id": event.sale_id,
                        "attempt": attempt,
                        "max_attempts": self._publish_attempts,
                        "error": str(exc),
                    },
                )
                if attempt < self._publish_attempts:
                    time.sleep(self._publish_retry_backoff_seconds)

        raise KafkaSalesPublishError(
            f"failed to publish sales event {event.event_id} after {self._publish_attempts} attempts"
        )
