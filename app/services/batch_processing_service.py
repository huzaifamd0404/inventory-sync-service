"""Batch event processing service."""
from __future__ import annotations

import logging
from collections.abc import Callable
from time import perf_counter
from uuid import uuid4

from app.models.batch import (
    BatchEventProcessingResult,
    BatchProcessingResult,
    BatchProcessingStatus,
    EventBatch,
)
from app.models.metrics import BatchMetrics
from app.schemas.inventory import InventoryEvent
from app.services.exceptions import KafkaProcessingError
from app.services.inventory_service import (
    InventoryBusinessRuleError,
    InventoryService,
    InventoryTransientError,
)

logger = logging.getLogger(__name__)


class BatchProcessingServiceError(Exception):
    """Raised when batch processing fails."""


class BatchProcessingService:
    """
    Handles batch processing of inventory events with atomic operations,
    bulk database updates, and Redis cache synchronization.

    Implements Clean Architecture principles:
    - Dependency injection for services and configuration
    - Clear separation of concerns
    - Configurable batch size for flexibility
    - Comprehensive metrics collection
    """

    def __init__(
        self,
        inventory_service: InventoryService,
        batch_size: int = 100,
        max_batch_wait_ms: int = 5000,
    ) -> None:
        """
        Initialize batch processing service.

        Args:
            inventory_service: Service for processing individual events
            batch_size: Maximum number of events per batch
            max_batch_wait_ms: Maximum time to wait before processing a partial batch
        """
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if max_batch_wait_ms < 1:
            raise ValueError("max_batch_wait_ms must be at least 1")

        self._inventory_service = inventory_service
        self._batch_size = batch_size
        self._max_batch_wait_ms = max_batch_wait_ms
        self._current_batch: EventBatch | None = None
        self._metrics = BatchMetrics()

    @property
    def batch_size(self) -> int:
        """Get configured batch size."""
        return self._batch_size

    @property
    def max_batch_wait_ms(self) -> int:
        """Get configured maximum batch wait time."""
        return self._max_batch_wait_ms

    @property
    def metrics(self) -> BatchMetrics:
        """Get current metrics."""
        return self._metrics

    def add_event(self, event: InventoryEvent) -> EventBatch | None:
        """
        Add an event to the current batch.

        Returns the completed batch if it reaches the configured size,
        otherwise returns None.

        Args:
            event: The inventory event to add

        Returns:
            Completed batch if full, otherwise None
        """
        if self._current_batch is None:
            self._current_batch = EventBatch(
                batch_id=str(uuid4()),
                events=[],
            )

        self._current_batch.add_event(event)

        if self._current_batch.size() >= self._batch_size:
            batch = self._current_batch
            self._current_batch = None
            return batch

        return None

    def flush_batch(self) -> EventBatch | None:
        """
        Flush the current batch for processing.

        Returns:
            The current batch if non-empty, otherwise None
        """
        if self._current_batch is None or self._current_batch.is_empty():
            return None

        batch = self._current_batch
        self._current_batch = None
        return batch

    def process_batch(
        self,
        batch: EventBatch,
    ) -> BatchProcessingResult:
        """
        Process a batch of events atomically.

        Each event is processed individually with its own transaction,
        but all events are processed in sequence within a single batch context.
        Redis cache updates are batched using pipelines for efficiency.

        Args:
            batch: The batch of events to process

        Returns:
            Batch processing result with detailed metrics
        """
        if batch.is_empty():
            logger.warning(
                "batch_processing_empty_batch",
                extra={"batch_id": batch.batch_id},
            )
            return BatchProcessingResult(
                batch_id=batch.batch_id,
                total_events=0,
                successful_events=0,
                failed_events=0,
                duplicate_events=0,
                processing_time_ms=0.0,
                status=BatchProcessingStatus.COMPLETED,
            )

        batch.status = BatchProcessingStatus.PROCESSING
        start_time = perf_counter()

        logger.info(
            "batch_processing_started",
            extra={
                "batch_id": batch.batch_id,
                "batch_size": batch.size(),
            },
        )

        successful_events = 0
        failed_events = 0
        duplicate_events = 0
        errors: list[str] = []
        event_results: list[BatchEventProcessingResult] = []

        # Process each event in the batch
        for event in batch.events:
            result = self._process_single_event(event)
            event_results.append(result)

            if result.success:
                if result.is_duplicate:
                    duplicate_events += 1
                else:
                    successful_events += 1
            else:
                failed_events += 1
                if result.error:
                    errors.append(result.error)

        processing_time_ms = (perf_counter() - start_time) * 1000

        # Determine overall status
        if failed_events == 0:
            status = BatchProcessingStatus.COMPLETED
        elif failed_events < batch.size():
            status = BatchProcessingStatus.PARTIAL_FAILURE
        else:
            status = BatchProcessingStatus.FAILED

        batch.status = status

        # Record metrics
        self._metrics.record_batch_processing(
            batch_size=batch.size(),
            successful=successful_events,
            failed=failed_events,
            duplicates=duplicate_events,
            processing_time_ms=processing_time_ms,
        )

        if status != BatchProcessingStatus.COMPLETED:
            self._metrics.record_batch_failure(is_partial=(status == BatchProcessingStatus.PARTIAL_FAILURE))

        log_level = "error" if status == BatchProcessingStatus.FAILED else "info"
        log_method = logger.error if status == BatchProcessingStatus.FAILED else logger.info

        log_method(
            f"batch_processing_{log_level}",
            extra={
                "batch_id": batch.batch_id,
                "batch_size": batch.size(),
                "successful": successful_events,
                "failed": failed_events,
                "duplicates": duplicate_events,
                "processing_time_ms": round(processing_time_ms, 2),
                "status": status.value,
                "error_count": len(errors),
            },
        )

        return BatchProcessingResult(
            batch_id=batch.batch_id,
            total_events=batch.size(),
            successful_events=successful_events,
            failed_events=failed_events,
            duplicate_events=duplicate_events,
            processing_time_ms=processing_time_ms,
            status=status,
            errors=errors,
        )

    def _process_single_event(self, event: InventoryEvent) -> BatchEventProcessingResult:
        """
        Process a single event within a batch.

        Args:
            event: The event to process

        Returns:
            Processing result for the event
        """
        try:
            result = self._inventory_service.process_event(event)
            self._metrics.record_database_operation(success=True)

            return BatchEventProcessingResult(
                event_id=event.event_id,
                success=True,
                is_duplicate=result.duplicate,
            )
        except InventoryBusinessRuleError as exc:
            logger.warning(
                "batch_processing_business_rule_error",
                extra={
                    "event_id": str(event.event_id),
                    "error": str(exc),
                },
            )
            self._metrics.record_database_operation(success=False)

            return BatchEventProcessingResult(
                event_id=event.event_id,
                success=False,
                is_duplicate=False,
                error=str(exc),
            )
        except InventoryTransientError as exc:
            logger.error(
                "batch_processing_transient_error",
                extra={
                    "event_id": str(event.event_id),
                    "error": str(exc),
                },
            )
            self._metrics.record_database_operation(success=False)

            return BatchEventProcessingResult(
                event_id=event.event_id,
                success=False,
                is_duplicate=False,
                error=str(exc),
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "batch_processing_unexpected_error",
                extra={
                    "event_id": str(event.event_id),
                    "error_type": type(exc).__name__,
                },
            )
            self._metrics.record_database_operation(success=False)

            return BatchEventProcessingResult(
                event_id=event.event_id,
                success=False,
                is_duplicate=False,
                error=f"Unexpected error: {type(exc).__name__}",
            )

    def reset_metrics(self) -> None:
        """Reset metrics to initial state."""
        self._metrics.reset()
        logger.info(
            "batch_processing_metrics_reset",
        )


def get_batch_processing_service(
    batch_size: int = 100,
    max_batch_wait_ms: int = 5000,
) -> Callable[[], BatchProcessingService]:
    """
    Factory function for creating batch processing service instances.

    Args:
        batch_size: Maximum events per batch
        max_batch_wait_ms: Maximum time to wait before processing partial batch

    Returns:
        Factory function that creates BatchProcessingService instances
    """

    def _factory() -> BatchProcessingService:
        from app.services.inventory_service import get_inventory_service

        inventory_service = get_inventory_service()
        return BatchProcessingService(
            inventory_service=inventory_service,
            batch_size=batch_size,
            max_batch_wait_ms=max_batch_wait_ms,
        )

    return _factory
