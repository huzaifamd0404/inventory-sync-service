from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaError
from pydantic import ValidationError

from app.config.settings import Settings, get_settings
from app.consumer.inventory_event_validator import InventoryEventValidator
from app.schemas.inventory import InventoryEvent
from app.services.exceptions import (
    DuplicateEvent,
    InvalidInventoryEvent,
    KafkaProcessingError,
)
from app.services.processed_event_service import (
    ProcessedEventService,
    get_processed_event_service,
)
from app.services.inventory_service import (
    InventoryBusinessRuleError,
    InventoryService,
    InventoryTransientError,
    get_inventory_service,
)

logger = logging.getLogger(__name__)


def build_kafka_consumer(group_id: str | None = None) -> KafkaConsumer:
    settings = get_settings()
    configured_group_id = group_id or settings.kafka_consumer_group_id
    return KafkaConsumer(
        settings.kafka_topic_inventory_updates,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=configured_group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )


class KafkaInventoryConsumer:
    def __init__(
        self,
        consumer: KafkaConsumer,
        inventory_service: InventoryService,
        processed_event_service: ProcessedEventService,
        event_validator: InventoryEventValidator,
        max_attempts: int,
        retry_backoff_seconds: float,
    ) -> None:
        self._consumer = consumer
        self._inventory_service = inventory_service
        self._processed_event_service = processed_event_service
        self._event_validator = event_validator
        self._max_attempts = max_attempts
        self._retry_backoff_seconds = retry_backoff_seconds

    @classmethod
    def from_settings(
        cls,
        settings: Settings | None = None,
        consumer_factory: Callable[[str | None], KafkaConsumer] = build_kafka_consumer,
        inventory_service_factory: Callable[[], InventoryService] = get_inventory_service,
        processed_event_service_factory: Callable[
            [], ProcessedEventService
        ] = get_processed_event_service,
        event_validator_factory: Callable[[], InventoryEventValidator] = InventoryEventValidator,
    ) -> "KafkaInventoryConsumer":
        configured_settings = settings or get_settings()
        consumer = consumer_factory(configured_settings.kafka_consumer_group_id)
        return cls(
            consumer=consumer,
            inventory_service=inventory_service_factory(),
            processed_event_service=processed_event_service_factory(),
            event_validator=event_validator_factory(),
            max_attempts=configured_settings.kafka_consumer_max_attempts,
            retry_backoff_seconds=configured_settings.kafka_consumer_retry_backoff_seconds,
        )

    def consume_forever(self) -> None:
        logger.info("kafka_inventory_consumer_started")
        for message in self._consumer:
            self.process_record(message)

    def process_record(self, message: ConsumerRecord) -> None:
        event = self._deserialize_message(message)
        if event is None:
            self._safe_commit(message, reason="invalid_event_payload")
            return

        try:
            self._event_validator.validate(event)
        except InvalidInventoryEvent as exc:
            logger.warning(
                "inventory_event_validation_failed",
                extra={
                    **self._message_context(message),
                    **self._event_context(event),
                    "error": str(exc),
                },
            )
            self._safe_commit(message, reason="invalid_event_payload")
            return

        for attempt in range(1, self._max_attempts + 1):
            try:
                self._processed_event_service.assert_not_processed(str(event.event_id))
                result = self._inventory_service.process_event(event)
                self._processed_event_service.mark_processed(event)
                self._commit_with_logging(message, reason="event_processed")
                logger.info(
                    "inventory_event_processed",
                    extra={
                        **self._message_context(message),
                        "event_id": result.event_id,
                        "product_id": result.product_id,
                        "store_id": result.store_id,
                        "operation": result.operation,
                        "quantity_before": result.quantity_before,
                        "quantity_after": result.quantity_after,
                        "quantity_delta": result.quantity_delta,
                        "duplicate": result.duplicate,
                    },
                )
                return
            except DuplicateEvent:
                logger.info(
                    "inventory_event_duplicate_skipped",
                    extra={**self._message_context(message), **self._event_context(event)},
                )
                self._safe_commit(message, reason="duplicate_event")
                return
            except InventoryTransientError as exc:
                logger.warning(
                    "inventory_event_processing_retry",
                    extra={
                        **self._message_context(message),
                        **self._event_context(event),
                        "attempt": attempt,
                        "max_attempts": self._max_attempts,
                        "error": str(exc),
                    },
                )
                if attempt == self._max_attempts:
                    logger.exception(
                        "inventory_event_processing_failed_transient",
                        extra={**self._message_context(message), **self._event_context(event)},
                    )
                    return
                time.sleep(self._retry_backoff_seconds * attempt)
            except InventoryBusinessRuleError as exc:
                logger.error(
                    "inventory_event_processing_failed_non_retryable",
                    extra={
                        **self._message_context(message),
                        **self._event_context(event),
                        "error": str(exc),
                    },
                )
                self._safe_commit(message, reason="non_retryable_failure")
                return
            except KafkaProcessingError:
                logger.exception(
                    "inventory_event_kafka_processing_failed",
                    extra={**self._message_context(message), **self._event_context(event)},
                )
                return
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "inventory_event_processing_failed_unexpected",
                    extra={
                        **self._message_context(message),
                        **self._event_context(event),
                        "attempt": attempt,
                        "max_attempts": self._max_attempts,
                        "error": str(exc),
                    },
                )
                if attempt == self._max_attempts:
                    return
                time.sleep(self._retry_backoff_seconds * attempt)

    def _deserialize_message(self, message: ConsumerRecord) -> InventoryEvent | None:
        try:
            value: Any = message.value
            return InventoryEvent.model_validate(value)
        except ValidationError as exc:
            logger.warning(
                "inventory_event_deserialization_failed",
                extra={
                    **self._message_context(message),
                    "payload": message.value,
                    "error": str(exc),
                },
            )
            return None

    def _commit_with_logging(self, message: ConsumerRecord, reason: str) -> None:
        try:
            self._consumer.commit()
            logger.debug(
                "kafka_offset_committed",
                extra={
                    **self._message_context(message),
                    "reason": reason,
                },
            )
        except KafkaError as exc:
            logger.exception(
                "kafka_offset_commit_failed",
                extra={
                    **self._message_context(message),
                    "reason": reason,
                },
            )
            raise KafkaProcessingError("kafka offset commit failed") from exc

    def _safe_commit(self, message: ConsumerRecord, reason: str) -> None:
        try:
            self._commit_with_logging(message, reason=reason)
        except KafkaProcessingError:
            logger.exception(
                "kafka_safe_commit_failed",
                extra={**self._message_context(message), "reason": reason},
            )

    @staticmethod
    def _message_context(message: ConsumerRecord) -> dict[str, object]:
        return {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
        }

    @staticmethod
    def _event_context(event: InventoryEvent) -> dict[str, object]:
        return {
            "event_id": str(event.event_id),
            "product_id": event.product_id,
            "store_id": event.store_id,
        }
