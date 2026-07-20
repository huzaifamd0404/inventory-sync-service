"""Schemas for metrics responses."""
from datetime import datetime

from pydantic import BaseModel, Field


class BatchMetricsResponse(BaseModel):
    """Response model for batch processing metrics."""

    total_batches_processed: int = Field(
        description="Total number of batches processed since service start"
    )
    total_events_processed: int = Field(
        description="Total number of events processed across all batches"
    )
    total_successful_events: int = Field(
        description="Total number of successfully processed events"
    )
    total_failed_events: int = Field(
        description="Total number of events that failed processing"
    )
    total_duplicate_events: int = Field(
        description="Total number of duplicate events encountered"
    )
    total_processing_time_ms: float = Field(
        description="Cumulative time spent processing all batches (milliseconds)"
    )
    min_batch_processing_time_ms: float = Field(
        description="Minimum time taken to process a batch (milliseconds)"
    )
    max_batch_processing_time_ms: float = Field(
        description="Maximum time taken to process a batch (milliseconds)"
    )
    avg_batch_processing_time_ms: float = Field(
        description="Average time taken to process a batch (milliseconds)"
    )
    total_batch_failures: int = Field(
        description="Total number of batches that failed completely"
    )
    total_partial_failures: int = Field(
        description="Total number of batches with partial failures"
    )
    total_redis_pipeline_operations: int = Field(
        description="Total number of Redis pipeline operations executed"
    )
    total_redis_pipeline_errors: int = Field(
        description="Total number of Redis pipeline operation errors"
    )
    total_database_operations: int = Field(
        description="Total number of database operations executed"
    )
    total_database_errors: int = Field(
        description="Total number of database operation errors"
    )
    last_updated_at: datetime = Field(
        description="ISO 8601 timestamp of last metrics update"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "total_batches_processed": 42,
                "total_events_processed": 4200,
                "total_successful_events": 4150,
                "total_failed_events": 30,
                "total_duplicate_events": 20,
                "total_processing_time_ms": 12500.0,
                "min_batch_processing_time_ms": 200.0,
                "max_batch_processing_time_ms": 400.0,
                "avg_batch_processing_time_ms": 297.62,
                "total_batch_failures": 0,
                "total_partial_failures": 3,
                "total_redis_pipeline_operations": 4200,
                "total_redis_pipeline_errors": 5,
                "total_database_operations": 4200,
                "total_database_errors": 30,
                "last_updated_at": "2026-07-20T14:30:45.123456+00:00",
            }
        }
