"""Metrics models for monitoring batch processing."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class BatchMetrics:
    """Tracks metrics for batch processing operations."""

    total_batches_processed: int = field(default=0)
    total_events_processed: int = field(default=0)
    total_successful_events: int = field(default=0)
    total_failed_events: int = field(default=0)
    total_duplicate_events: int = field(default=0)
    total_processing_time_ms: float = field(default=0.0)

    # Performance metrics
    min_batch_processing_time_ms: float = field(default=float("inf"))
    max_batch_processing_time_ms: float = field(default=0.0)
    avg_batch_processing_time_ms: float = field(default=0.0)

    # Error tracking
    total_batch_failures: int = field(default=0)
    total_partial_failures: int = field(default=0)

    # Cache metrics
    total_redis_pipeline_operations: int = field(default=0)
    total_redis_pipeline_errors: int = field(default=0)

    # Database metrics
    total_database_operations: int = field(default=0)
    total_database_errors: int = field(default=0)

    # Last update
    last_updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def record_batch_processing(
        self,
        batch_size: int,
        successful: int,
        failed: int,
        duplicates: int,
        processing_time_ms: float,
    ) -> None:
        """Record metrics for a completed batch."""
        self.total_batches_processed += 1
        self.total_events_processed += batch_size
        self.total_successful_events += successful
        self.total_failed_events += failed
        self.total_duplicate_events += duplicates
        self.total_processing_time_ms += processing_time_ms

        # Update min/max
        if processing_time_ms < self.min_batch_processing_time_ms:
            self.min_batch_processing_time_ms = processing_time_ms
        if processing_time_ms > self.max_batch_processing_time_ms:
            self.max_batch_processing_time_ms = processing_time_ms

        # Update average
        self.avg_batch_processing_time_ms = (
            self.total_processing_time_ms / self.total_batches_processed
        )
        self.last_updated_at = datetime.now(UTC)

    def record_batch_failure(self, is_partial: bool = False) -> None:
        """Record a batch failure."""
        if is_partial:
            self.total_partial_failures += 1
        else:
            self.total_batch_failures += 1
        self.last_updated_at = datetime.now(UTC)

    def record_redis_operation(self, success: bool = True) -> None:
        """Record a Redis pipeline operation."""
        self.total_redis_pipeline_operations += 1
        if not success:
            self.total_redis_pipeline_errors += 1
        self.last_updated_at = datetime.now(UTC)

    def record_database_operation(self, success: bool = True) -> None:
        """Record a database operation."""
        self.total_database_operations += 1
        if not success:
            self.total_database_errors += 1
        self.last_updated_at = datetime.now(UTC)

    def reset(self) -> None:
        """Reset all metrics to initial state."""
        self.total_batches_processed = 0
        self.total_events_processed = 0
        self.total_successful_events = 0
        self.total_failed_events = 0
        self.total_duplicate_events = 0
        self.total_processing_time_ms = 0.0
        self.min_batch_processing_time_ms = float("inf")
        self.max_batch_processing_time_ms = 0.0
        self.avg_batch_processing_time_ms = 0.0
        self.total_batch_failures = 0
        self.total_partial_failures = 0
        self.total_redis_pipeline_operations = 0
        self.total_redis_pipeline_errors = 0
        self.total_database_operations = 0
        self.total_database_errors = 0
        self.last_updated_at = datetime.now(UTC)

    def to_dict(self) -> dict:
        """Convert metrics to dictionary for serialization."""
        return {
            "total_batches_processed": self.total_batches_processed,
            "total_events_processed": self.total_events_processed,
            "total_successful_events": self.total_successful_events,
            "total_failed_events": self.total_failed_events,
            "total_duplicate_events": self.total_duplicate_events,
            "total_processing_time_ms": round(self.total_processing_time_ms, 2),
            "min_batch_processing_time_ms": (
                round(self.min_batch_processing_time_ms, 2)
                if self.min_batch_processing_time_ms != float("inf")
                else 0.0
            ),
            "max_batch_processing_time_ms": round(self.max_batch_processing_time_ms, 2),
            "avg_batch_processing_time_ms": round(self.avg_batch_processing_time_ms, 2),
            "total_batch_failures": self.total_batch_failures,
            "total_partial_failures": self.total_partial_failures,
            "total_redis_pipeline_operations": self.total_redis_pipeline_operations,
            "total_redis_pipeline_errors": self.total_redis_pipeline_errors,
            "total_database_operations": self.total_database_operations,
            "total_database_errors": self.total_database_errors,
            "last_updated_at": self.last_updated_at.isoformat(),
        }
