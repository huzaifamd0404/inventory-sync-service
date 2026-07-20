"""Batch processing domain models."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from uuid import UUID

from app.schemas.inventory import InventoryEvent


class BatchProcessingStatus(str, Enum):
    """Status of batch processing."""

    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL_FAILURE = "partial_failure"


@dataclass(frozen=True)
class BatchProcessingResult:
    """Result of processing a batch of events."""

    batch_id: str
    total_events: int
    successful_events: int
    failed_events: int
    duplicate_events: int
    processing_time_ms: float
    status: BatchProcessingStatus
    errors: list[str] = field(default_factory=list)


@dataclass
class EventBatch:
    """A collection of inventory events to be processed atomically."""

    batch_id: str
    events: list[InventoryEvent]
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    status: BatchProcessingStatus = field(default=BatchProcessingStatus.QUEUED)

    def size(self) -> int:
        """Return the number of events in the batch."""
        return len(self.events)

    def is_empty(self) -> bool:
        """Check if the batch is empty."""
        return len(self.events) == 0

    def add_event(self, event: InventoryEvent) -> None:
        """Add an event to the batch."""
        self.events.append(event)

    def add_events(self, events: list[InventoryEvent]) -> None:
        """Add multiple events to the batch."""
        self.events.extend(events)

    def clear(self) -> None:
        """Clear all events from the batch."""
        self.events.clear()
        self.status = BatchProcessingStatus.QUEUED


@dataclass(frozen=True)
class BatchEventProcessingResult:
    """Result of processing a single event within a batch."""

    event_id: UUID
    success: bool
    is_duplicate: bool
    error: str | None = None
