import json
import logging
import time
from collections.abc import Callable
from typing import Any, Protocol

from kafka import KafkaProducer
from kafka.errors import KafkaError

from app.config.settings import Settings, get_settings
from app.schemas.inventory import InventoryEvent

logger = logging.getLogger(__name__)


class InventoryEventPublisher(Protocol):
    def publish_inventory_event(self, event: InventoryEvent) -> None: ...


class KafkaProducerInitializationError(Exception):
    """Raised when Kafka producer setup fails."""


class KafkaPublishError(Exception):
    """Raised when publishing to Kafka fails after all retries."""


class KafkaInventoryProducer(InventoryEventPublisher):
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
    ) -> "KafkaInventoryProducer":
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
                value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode(
                    "utf-8"
                ),
            )
        except KafkaError as exc:
            logger.exception("kafka_producer_initialization_failed")
            raise KafkaProducerInitializationError("failed to initialize Kafka producer") from exc

        return cls(
            producer=producer,
            topic=configured_settings.kafka_topic_inventory_updates,
            publish_attempts=configured_settings.kafka_publish_attempts,
            publish_retry_backoff_seconds=configured_settings.kafka_publish_retry_backoff_seconds,
            publish_timeout_seconds=configured_settings.kafka_publish_timeout_seconds,
        )

    def publish_inventory_event(self, event: InventoryEvent) -> None:
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
                    "inventory_event_published",
                    extra={
                        "event_id": str(event.event_id),
                        "product_id": event.product_id,
                        "store_id": event.store_id,
                        "topic": self._topic,
                        "partition": record_metadata.partition,
                        "offset": record_metadata.offset,
                    },
                )
                return
            except KafkaError as exc:
                logger.warning(
                    "inventory_event_publish_retry",
                    extra={
                        "event_id": str(event.event_id),
                        "topic": self._topic,
                        "attempt": attempt,
                        "max_attempts": self._publish_attempts,
                        "error": str(exc),
                    },
                )
                if attempt == self._publish_attempts:
                    logger.exception(
                        "inventory_event_publish_failed",
                        extra={"event_id": str(event.event_id), "topic": self._topic},
                    )
                    raise KafkaPublishError("failed to publish inventory event") from exc
                time.sleep(self._publish_retry_backoff_seconds)
