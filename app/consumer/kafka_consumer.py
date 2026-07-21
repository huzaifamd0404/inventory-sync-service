from __future__ import annotations

import json
import logging
from collections.abc import Callable
from time import perf_counter
from typing import Any

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaError
from pydantic import ValidationError

from app.config.settings import Settings, get_settings
from app.consumer.inventory_event_validator import InventoryEventValidator
from app.observability.metrics import (
    observe_processing_duration,
    record_dlq_events,
    record_duplicate_events,
    record_failed_events,
    record_processed_events,
    record_retried_events,
)
from app.producer.dlq_producer import DeadLetterQueuePublisher, KafkaDlqProducer, KafkaDlqPublishError
from app.schemas.inventory import InventoryEvent
from app.services.exceptions import (
    DuplicateEvent,
    InvalidInventoryEvent,
    KafkaProcessingError,
)
from app.services.failed_event_service import FailedEventService, get_failed_event_service
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
from app.services.retry_service import RetryService

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
        failed_event_service: FailedEventService,
        dlq_publisher: DeadLetterQueuePublisher,
        event_validator: InventoryEventValidator,
        retry_service: RetryService,
    ) -> None:
        self._consumer = consumer
        self._inventory_service = inventory_service
        self._processed_event_service = processed_event_service
        self._failed_event_service = failed_event_service
        self._dlq_publisher = dlq_publisher
        self._event_validator = event_validator
        self._retry_service = retry_service
        self._stop_requested = False

    @classmethod
    def from_settings(
        cls,
        settings: Settings | None = None,
        consumer_factory: Callable[[str | None], KafkaConsumer] = build_kafka_consumer,
        inventory_service_factory: Callable[[], InventoryService] = get_inventory_service,
        processed_event_service_factory: Callable[
            [], ProcessedEventService
        ] = get_processed_event_service,
        failed_event_service_factory: Callable[[], FailedEventService] = get_failed_event_service,
        dlq_publisher_factory: Callable[[Settings], DeadLetterQueuePublisher] = (
            lambda configured_settings: KafkaDlqProducer.from_settings(settings=configured_settings)
        ),
        event_validator_factory: Callable[[], InventoryEventValidator] = InventoryEventValidator,
    ) -> "KafkaInventoryConsumer":
        configured_settings = settings or get_settings()
        consumer = consumer_factory(configured_settings.kafka_consumer_group_id)
        return cls(
            consumer=consumer,
            inventory_service=inventory_service_factory(),
            processed_event_service=processed_event_service_factory(),
            failed_event_service=failed_event_service_factory(),
            dlq_publisher=dlq_publisher_factory(configured_settings),
            event_validator=event_validator_factory(),
            retry_service=RetryService(
                max_attempts=configured_settings.kafka_consumer_max_attempts,
                initial_backoff_seconds=configured_settings.kafka_consumer_retry_initial_backoff_seconds,
                backoff_multiplier=configured_settings.kafka_consumer_retry_backoff_multiplier,
                max_backoff_seconds=configured_settings.kafka_consumer_retry_max_backoff_seconds,
            ),
        )

    def consume_forever(self) -> None:
        logger.info("kafka_inventory_consumer_started")
        try:
            for message in self._consumer:
                if self._stop_requested:
                    logger.info("kafka_inventory_consumer_stop_requested")
                    break
                self.process_record(message)
        finally:
            self.close()

    def stop(self) -> None:
        self._stop_requested = True

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception:  # noqa: BLE001
            logger.exception("kafka_inventory_consumer_close_failed")

    def process_record(self, message: ConsumerRecord) -> None:
        started = perf_counter()
        outcome = "failed"
        payload: object = message.value
        event = self._deserialize_message(message)
        if event is None:
            self._handle_permanent_failure(
                message=message,
                event=None,
                payload=payload,
                failure_reason="invalid_event_payload",
                retry_count=0,
            )
            observe_processing_duration(perf_counter() - started, outcome=outcome)
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
            self._handle_permanent_failure(
                message=message,
                event=event,
                payload=payload,
                failure_reason=f"invalid_inventory_event:{exc}",
                retry_count=0,
            )
            return

        try:
            result, retry_result = self._retry_service.execute(
                operation=lambda: self._process_event_once(event),
                retryable_exceptions=(InventoryTransientError,),
                operation_name="inventory_event_processing",
                on_retry=lambda attempt, exc, backoff_seconds: logger.warning(
                    "inventory_event_processing_retry",
                    extra={
                        **self._message_context(message),
                        **self._event_context(event),
                        "attempt": attempt,
                        "max_attempts": self._retry_service.max_attempts,
                        "next_backoff_seconds": backoff_seconds,
                        "error": str(exc),
                    },
                ),
            )
            retry_count = retry_result.attempts - 1
            if retry_count > 0:
                record_retried_events(retry_count)

            self._commit_with_logging(message, reason="event_processed")
            record_processed_events()
            outcome = "processed"
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
                    "retry_count": retry_count,
                    "processing_time": round((perf_counter() - started) * 1000, 2),
                },
            )
        except DuplicateEvent:
            record_duplicate_events()
            outcome = "duplicate"
            logger.info(
                "inventory_event_duplicate_skipped",
                extra={**self._message_context(message), **self._event_context(event)},
            )
            self._safe_commit(message, reason="duplicate_event")
        except InventoryBusinessRuleError as exc:
            logger.error(
                "inventory_event_processing_failed_non_retryable",
                extra={
                    **self._message_context(message),
                    **self._event_context(event),
                    "error": str(exc),
                },
            )
            self._handle_permanent_failure(
                message=message,
                event=event,
                payload=payload,
                failure_reason=f"non_retryable_failure:{exc}",
                retry_count=0,
            )
        except InventoryTransientError as exc:
            logger.exception(
                "inventory_event_processing_failed_transient_exhausted",
                extra={**self._message_context(message), **self._event_context(event)},
            )
            self._handle_permanent_failure(
                message=message,
                event=event,
                payload=payload,
                failure_reason=f"transient_failure_after_retries:{exc}",
                retry_count=self._retry_service.max_attempts,
            )
        except KafkaProcessingError:
            outcome = "kafka_error"
            logger.exception(
                "inventory_event_kafka_processing_failed",
                extra={**self._message_context(message), **self._event_context(event)},
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "inventory_event_processing_failed_unexpected",
                extra={
                    **self._message_context(message),
                    **self._event_context(event),
                    "error": str(exc),
                },
            )
            self._handle_permanent_failure(
                message=message,
                event=event,
                payload=payload,
                failure_reason=f"unexpected_failure:{exc}",
                retry_count=0,
            )
        finally:
            observe_processing_duration(perf_counter() - started, outcome=outcome)

    def _process_event_once(self, event: InventoryEvent) -> Any:
        self._processed_event_service.assert_not_processed(str(event.event_id))
        result = self._inventory_service.process_event(event)
        self._processed_event_service.mark_processed(event)
        return result

    def _handle_permanent_failure(
        self,
        *,
        message: ConsumerRecord,
        event: InventoryEvent | None,
        payload: object,
        failure_reason: str,
        retry_count: int,
    ) -> None:
        event_id = str(event.event_id) if event is not None else self._extract_event_id(payload)
        record_failed_events()

        try:
            failed_event = self._failed_event_service.record_failure(
                event_id=event_id,
                source_topic=message.topic,
                source_partition=message.partition,
                source_offset=message.offset,
                payload=payload,
                failure_reason=failure_reason,
                retry_count=retry_count,
            )
            logger.info(
                "inventory_event_failure_persisted",
                extra={
                    **self._message_context(message),
                    "event_id": event_id,
                    "failed_event_id": str(failed_event.id),
                    "retry_count": retry_count,
                    "failure_reason": failure_reason,
                },
            )
            self._dlq_publisher.publish_failed_event(
                event_id=event_id,
                source_topic=message.topic,
                source_partition=message.partition,
                source_offset=message.offset,
                payload=payload,
                failure_reason=failure_reason,
                retry_count=retry_count,
            )
            record_dlq_events()
            self._commit_with_logging(message, reason="failed_event_sent_to_dlq")
        except (InventoryTransientError, KafkaDlqPublishError):
            logger.exception(
                "inventory_event_failure_handling_incomplete",
                extra={
                    **self._message_context(message),
                    "event_id": event_id,
                    "retry_count": retry_count,
                    "failure_reason": failure_reason,
                },
            )

    @staticmethod
    def _extract_event_id(payload: object) -> str | None:
        if isinstance(payload, dict):
            raw_event_id = payload.get("event_id")
            if raw_event_id is None:
                return None
            return str(raw_event_id)
        return None

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
