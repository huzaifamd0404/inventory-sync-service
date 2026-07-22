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
from app.consumer.sales_event_validator import SalesEventValidator
from app.producer.dlq_producer import DeadLetterQueuePublisher, KafkaDlqProducer
from app.schemas.sales import SalesEvent
from app.services.exceptions import InvalidInventoryEvent
from app.services.retry_service import RetryService
from app.services.sales_service import (
    SalesBusinessRuleError,
    SalesService,
    SalesTransientError,
    get_sales_service,
)

logger = logging.getLogger(__name__)


def build_sales_kafka_consumer(group_id: str | None = None) -> KafkaConsumer:
    settings = get_settings()
    configured_group_id = group_id or settings.kafka_consumer_sales_group_id
    return KafkaConsumer(
        settings.kafka_topic_sales_events,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=configured_group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )


class KafkaSalesConsumer:
    def __init__(
        self,
        consumer: KafkaConsumer,
        sales_service: SalesService,
        dlq_publisher: DeadLetterQueuePublisher,
        event_validator: SalesEventValidator,
        retry_service: RetryService,
    ) -> None:
        self._consumer = consumer
        self._sales_service = sales_service
        self._dlq_publisher = dlq_publisher
        self._event_validator = event_validator
        self._retry_service = retry_service
        self._stop_requested = False

    @classmethod
    def from_settings(
        cls,
        settings: Settings | None = None,
        consumer_factory: Callable[[str | None], KafkaConsumer] = build_sales_kafka_consumer,
        sales_service_factory: Callable[[], SalesService] = get_sales_service,
        dlq_publisher_factory: Callable[[Settings], DeadLetterQueuePublisher] = (
            lambda configured_settings: KafkaDlqProducer.from_settings(
                settings=configured_settings
            )
        ),
        event_validator_factory: Callable[[], SalesEventValidator] = SalesEventValidator,
    ) -> "KafkaSalesConsumer":
        configured_settings = settings or get_settings()
        consumer = consumer_factory(configured_settings.kafka_consumer_sales_group_id)
        return cls(
            consumer=consumer,
            sales_service=sales_service_factory(),
            dlq_publisher=dlq_publisher_factory(configured_settings),
            event_validator=event_validator_factory(),
            retry_service=RetryService(
                max_attempts=configured_settings.kafka_consumer_max_attempts,
                initial_backoff_seconds=configured_settings.kafka_consumer_retry_initial_backoff_seconds,
                backoff_multiplier=configured_settings.kafka_consumer_retry_backoff_multiplier,
                max_backoff_seconds=configured_settings.kafka_consumer_retry_max_backoff_seconds,
            ),
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def consume_forever(self) -> None:
        logger.info("kafka_sales_consumer_started")
        try:
            for message in self._consumer:
                if self._stop_requested:
                    logger.info("kafka_sales_consumer_stop_requested")
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
            logger.exception("kafka_sales_consumer_close_failed")

    # ------------------------------------------------------------------
    # Record processing
    # ------------------------------------------------------------------

    def process_record(self, message: ConsumerRecord) -> None:
        started = perf_counter()
        payload: object = message.value

        event = self._deserialize_message(message)
        if event is None:
            self._handle_permanent_failure(
                message=message,
                payload=payload,
                failure_reason="invalid_sales_event_payload",
            )
            return

        try:
            self._event_validator.validate(event)
        except InvalidInventoryEvent as exc:
            logger.warning(
                "sales_event_validation_failed",
                extra={
                    **self._message_context(message),
                    "event_id": str(event.event_id),
                    "sale_id": event.sale_id,
                    "error": str(exc),
                },
            )
            self._handle_permanent_failure(
                message=message,
                payload=payload,
                failure_reason=f"invalid_sales_event:{exc}",
            )
            return

        try:
            result, retry_result = self._retry_service.execute(
                operation=lambda: self._sales_service.process_event(event),
                retryable_exceptions=(SalesTransientError,),
                operation_name="sales_event_processing",
                on_retry=lambda attempt, exc, backoff_seconds: logger.warning(
                    "sales_event_processing_retry",
                    extra={
                        **self._message_context(message),
                        "event_id": str(event.event_id),
                        "sale_id": event.sale_id,
                        "attempt": attempt,
                        "next_backoff_seconds": backoff_seconds,
                        "error": str(exc),
                    },
                ),
            )
            self._safe_commit(message)
            logger.info(
                "sales_event_processed",
                extra={
                    **self._message_context(message),
                    "event_id": str(event.event_id),
                    "sale_id": result.sale_id,
                    "product_id": result.product_id,
                    "store_id": result.store_id,
                    "quantity_sold": result.quantity_sold,
                    "duplicate": result.duplicate,
                    "attempts": retry_result.attempts,
                    "processing_ms": round((perf_counter() - started) * 1000, 2),
                },
            )
        except SalesBusinessRuleError as exc:
            logger.error(
                "sales_event_processing_failed_non_retryable",
                extra={
                    **self._message_context(message),
                    "event_id": str(event.event_id),
                    "sale_id": event.sale_id,
                    "error": str(exc),
                },
            )
            self._handle_permanent_failure(
                message=message,
                payload=payload,
                failure_reason=f"non_retryable_failure:{exc}",
            )
        except SalesTransientError as exc:
            logger.error(
                "sales_event_processing_failed_exhausted_retries",
                extra={
                    **self._message_context(message),
                    "event_id": str(event.event_id),
                    "sale_id": event.sale_id,
                    "error": str(exc),
                },
            )
            self._handle_permanent_failure(
                message=message,
                payload=payload,
                failure_reason=f"transient_exhausted:{exc}",
            )

    def _deserialize_message(self, message: ConsumerRecord) -> SalesEvent | None:
        raw: Any = message.value
        if not isinstance(raw, dict):
            logger.warning(
                "sales_consumer_unexpected_payload_type",
                extra={**self._message_context(message), "payload_type": type(raw).__name__},
            )
            return None
        try:
            return SalesEvent.model_validate(raw)
        except ValidationError as exc:
            logger.warning(
                "sales_consumer_deserialization_failed",
                extra={**self._message_context(message), "errors": exc.errors()},
            )
            return None

    def _safe_commit(self, message: ConsumerRecord) -> None:
        try:
            self._consumer.commit()
        except KafkaError:
            logger.exception(
                "sales_consumer_commit_failed",
                extra=self._message_context(message),
            )

    def _handle_permanent_failure(
        self,
        message: ConsumerRecord,
        payload: object,
        failure_reason: str,
    ) -> None:
        try:
            self._dlq_publisher.publish_failed_event(
                event_id=None,
                source_topic=message.topic,
                source_partition=message.partition,
                source_offset=message.offset,
                payload=payload,
                failure_reason=failure_reason,
                retry_count=0,
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "sales_consumer_dlq_publish_failed",
                extra=self._message_context(message),
            )
        self._safe_commit(message)

    @staticmethod
    def _message_context(message: ConsumerRecord) -> dict[str, object]:
        return {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
        }
