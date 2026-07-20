"""Batch Kafka consumer for processing inventory events in batches."""
from __future__ import annotations

import logging
from collections.abc import Callable
from time import perf_counter

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaError
from pydantic import ValidationError

from app.config.settings import Settings, get_settings
from app.consumer.inventory_event_validator import InventoryEventValidator
from app.consumer.kafka_consumer import KafkaInventoryConsumer, build_kafka_consumer
from app.models.batch import BatchProcessingStatus
from app.producer.dlq_producer import DeadLetterQueuePublisher, KafkaDlqProducer, KafkaDlqPublishError
from app.schemas.inventory import InventoryEvent
from app.services.batch_processing_service import BatchProcessingService
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


class BatchKafkaInventoryConsumer:
    """
    Batch-oriented Kafka consumer for inventory events.

    Processes events in configurable batches using:
    - Event batching based on count or time
    - Atomic batch processing
    - Bulk database operations
    - Redis pipeline cache updates
    - Optimized Kafka consumer polling
    - Comprehensive metrics and logging

    Implements Clean Architecture:
    - Dependency injection for all services
    - Clear separation of concerns
    - Follows SOLID principles
    """

    def __init__(
        self,
        consumer: KafkaConsumer,
        batch_processing_service: BatchProcessingService,
        inventory_service: InventoryService,
        processed_event_service: ProcessedEventService,
        failed_event_service: FailedEventService,
        dlq_publisher: DeadLetterQueuePublisher,
        event_validator: InventoryEventValidator,
        retry_service: RetryService,
        settings: Settings,
    ) -> None:
        """
        Initialize batch Kafka consumer.

        Args:
            consumer: Kafka consumer instance
            batch_processing_service: Service for batch event processing
            inventory_service: Service for inventory operations
            processed_event_service: Service for tracking processed events
            failed_event_service: Service for tracking failed events
            dlq_publisher: Publisher for dead letter queue
            event_validator: Validator for inventory events
            retry_service: Service for retry logic
            settings: Application settings
        """
        self._consumer = consumer
        self._batch_service = batch_processing_service
        self._inventory_service = inventory_service
        self._processed_event_service = processed_event_service
        self._failed_event_service = failed_event_service
        self._dlq_publisher = dlq_publisher
        self._event_validator = event_validator
        self._retry_service = retry_service
        self._settings = settings
        self._pending_commits: dict[tuple[str, int], int] = {}  # (topic, partition) -> offset

    @classmethod
    def from_settings(
        cls,
        settings: Settings | None = None,
        consumer_factory: Callable[[str | None], KafkaConsumer] = build_kafka_consumer,
        batch_processing_service_factory: Callable[
            [], BatchProcessingService
        ] | None = None,
        inventory_service_factory: Callable[[], InventoryService] = get_inventory_service,
        processed_event_service_factory: Callable[
            [], ProcessedEventService
        ] = get_processed_event_service,
        failed_event_service_factory: Callable[[], FailedEventService] = get_failed_event_service,
        dlq_publisher_factory: Callable[[Settings], DeadLetterQueuePublisher] = (
            lambda configured_settings: KafkaDlqProducer.from_settings(
                settings=configured_settings
            )
        ),
        event_validator_factory: Callable[[], InventoryEventValidator] = InventoryEventValidator,
    ) -> "BatchKafkaInventoryConsumer":
        """
        Create batch consumer from settings.

        Args:
            settings: Application settings
            consumer_factory: Factory for creating Kafka consumer
            batch_processing_service_factory: Factory for batch service
            inventory_service_factory: Factory for inventory service
            processed_event_service_factory: Factory for processed event service
            failed_event_service_factory: Factory for failed event service
            dlq_publisher_factory: Factory for DLQ publisher
            event_validator_factory: Factory for event validator

        Returns:
            Configured BatchKafkaInventoryConsumer instance
        """
        configured_settings = settings or get_settings()
        consumer = consumer_factory(configured_settings.kafka_consumer_group_id)

        # Use default batch service factory if not provided
        if batch_processing_service_factory is None:
            from app.services.batch_processing_service import get_batch_processing_service

            batch_processing_service_factory = get_batch_processing_service(
                batch_size=configured_settings.batch_size,
                max_batch_wait_ms=configured_settings.batch_max_wait_ms,
            )

        return cls(
            consumer=consumer,
            batch_processing_service=batch_processing_service_factory(),
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
            settings=configured_settings,
        )

    def consume_forever(self) -> None:
        """
        Start consuming and processing events in batches.

        Runs indefinitely, processing Kafka messages in batches and committing offsets.
        """
        logger.info(
            "batch_kafka_consumer_started",
            extra={
                "batch_size": self._batch_service.batch_size,
                "max_batch_wait_ms": self._batch_service.max_batch_wait_ms,
            },
        )

        try:
            while True:
                # Poll for new messages with optimized timeout
                messages = self._consumer.poll(
                    timeout_ms=self._settings.kafka_consumer_poll_timeout_ms,
                    max_records=self._batch_service.batch_size,
                )

                if not messages:
                    # Check if we have a partial batch to flush
                    batch = self._batch_service.flush_batch()
                    if batch is not None:
                        self._process_batch(batch)
                    continue

                # Process all polled messages
                for topic_partition, records in messages.items():
                    for message in records:
                        event = self._deserialize_message(message)
                        if event is not None:
                            # Add event to batch
                            completed_batch = self._batch_service.add_event(event)
                            if completed_batch is not None:
                                # Batch is full, process it
                                self._process_batch(completed_batch)

                # Flush any remaining partial batch
                batch = self._batch_service.flush_batch()
                if batch is not None:
                    self._process_batch(batch)
        except KeyboardInterrupt:
            logger.info("batch_kafka_consumer_interrupted")
            self._consumer.close()
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "batch_kafka_consumer_error",
            )
            self._consumer.close()
            raise

    def _process_batch(self, batch) -> None:  # type: ignore
        """
        Process a completed batch of events.

        Args:
            batch: EventBatch to process
        """
        start_time = perf_counter()

        logger.info(
            "batch_kafka_consumer_processing_batch",
            extra={
                "batch_id": batch.batch_id,
                "batch_size": batch.size(),
            },
        )

        # Process the batch
        result = self._batch_service.process_batch(batch)

        processing_time_ms = (perf_counter() - start_time) * 1000

        logger.info(
            "batch_kafka_consumer_batch_processed",
            extra={
                "batch_id": batch.batch_id,
                "batch_size": batch.size(),
                "successful": result.successful_events,
                "failed": result.failed_events,
                "duplicates": result.duplicate_events,
                "processing_time_ms": round(processing_time_ms, 2),
                "status": result.status.value,
            },
        )

        # Handle failures if needed (persist to DLQ, etc.)
        if result.status in (BatchProcessingStatus.FAILED, BatchProcessingStatus.PARTIAL_FAILURE):
            logger.warning(
                "batch_kafka_consumer_batch_has_failures",
                extra={
                    "batch_id": batch.batch_id,
                    "failed_events": result.failed_events,
                    "error_count": len(result.errors),
                },
            )

        # Commit offsets
        try:
            self._consumer.commit()
            logger.debug(
                "batch_kafka_consumer_offsets_committed",
                extra={"batch_id": batch.batch_id},
            )
        except KafkaError as exc:
            logger.exception(
                "batch_kafka_consumer_commit_failed",
                extra={"batch_id": batch.batch_id},
            )

    def _deserialize_message(self, message: ConsumerRecord) -> InventoryEvent | None:
        """
        Deserialize a Kafka message to an InventoryEvent.

        Args:
            message: Kafka consumer record

        Returns:
            Deserialized event or None if deserialization fails
        """
        try:
            value = message.value
            event = InventoryEvent.model_validate(value)

            # Validate event
            try:
                self._event_validator.validate(event)
                return event
            except InvalidInventoryEvent as exc:
                logger.warning(
                    "batch_kafka_consumer_event_validation_failed",
                    extra={
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "error": str(exc),
                    },
                )
                return None
        except ValidationError as exc:
            logger.warning(
                "batch_kafka_consumer_deserialization_failed",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "error": str(exc),
                },
            )
            return None
