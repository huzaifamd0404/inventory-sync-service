"""Kafka producer for publishing alerts."""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any, Protocol

from kafka import KafkaProducer
from kafka.errors import KafkaError

from app.config.settings import Settings, get_settings
from app.database.models import Alert

logger = logging.getLogger(__name__)


class AlertPublisher(Protocol):
    """Protocol for publishing alerts."""

    def publish_alert(self, alert: Alert) -> None: ...


class KafkaProducerInitializationError(Exception):
    """Raised when Kafka producer setup fails."""


class KafkaPublishError(Exception):
    """Raised when publishing to Kafka fails after all retries."""


class KafkaAlertProducer(AlertPublisher):
    """Kafka producer for publishing alerts to inventory_alerts topic."""

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
    ) -> "KafkaAlertProducer":
        """Create AlertProducer from settings.

        Args:
            settings: Settings instance (uses get_settings() if None)
            producer_factory: Factory for creating KafkaProducer instances

        Returns:
            Configured KafkaAlertProducer instance

        Raises:
            KafkaProducerInitializationError: If producer setup fails
        """
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
            logger.exception("kafka_alert_producer_initialization_failed")
            raise KafkaProducerInitializationError("failed to initialize Kafka producer") from exc

        return cls(
            producer=producer,
            topic=configured_settings.kafka_topic_alerts,
            publish_attempts=configured_settings.kafka_publish_attempts,
            publish_retry_backoff_seconds=configured_settings.kafka_publish_retry_backoff_seconds,
            publish_timeout_seconds=configured_settings.kafka_publish_timeout_seconds,
        )

    def publish_alert(self, alert: Alert) -> None:
        """Publish an alert to Kafka.

        Args:
            alert: Alert instance to publish

        Raises:
            KafkaPublishError: If publishing fails after all retries
        """
        payload = {
            "id": str(alert.id),
            "anomaly_id": str(alert.anomaly_id),
            "inventory_id": str(alert.inventory_id),
            "event_id": alert.event_id,
            "severity": alert.severity.value,
            "status": alert.status.value,
            "title": alert.title,
            "description": alert.description,
            "triggered_at": alert.triggered_at.isoformat(),
            "acknowledged_at": alert.acknowledged_at.isoformat() if alert.acknowledged_at else None,
            "acknowledged_by": alert.acknowledged_by,
            "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
            "suppressed_until": (
                alert.suppressed_until.isoformat() if alert.suppressed_until else None
            ),
        }

        for attempt in range(1, self._publish_attempts + 1):
            try:
                future: Any = self._producer.send(
                    self._topic,
                    value=payload,
                    key=str(alert.id).encode("utf-8"),
                )
                record_metadata: Any = future.get(timeout=self._publish_timeout_seconds)
                logger.info(
                    "alert_published",
                    extra={
                        "alert_id": str(alert.id),
                        "anomaly_id": str(alert.anomaly_id),
                        "inventory_id": str(alert.inventory_id),
                        "severity": alert.severity.value,
                        "topic": self._topic,
                        "partition": record_metadata.partition,
                        "offset": record_metadata.offset,
                    },
                )
                return
            except KafkaError as exc:
                logger.warning(
                    "alert_publish_retry",
                    extra={
                        "alert_id": str(alert.id),
                        "topic": self._topic,
                        "attempt": attempt,
                        "max_attempts": self._publish_attempts,
                        "error": str(exc),
                    },
                )
                if attempt == self._publish_attempts:
                    logger.exception(
                        "alert_publish_failed",
                        extra={"alert_id": str(alert.id), "topic": self._topic},
                    )
                    raise KafkaPublishError("failed to publish alert") from exc
                time.sleep(self._publish_retry_backoff_seconds)
