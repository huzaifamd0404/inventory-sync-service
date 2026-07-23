"""Pydantic schemas for anomaly detection API."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.database.models import AnomalySeverity, AnomalyStatus


class AnomalyResponse(BaseModel):
    """Response model for a single anomaly."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(description="Unique anomaly identifier")
    inventory_id: UUID = Field(description="Associated inventory item identifier")
    event_id: str | None = Field(default=None, description="Source event identifier")
    anomaly_type: str = Field(
        description="Type of anomaly detected",
        examples=["negative_inventory", "sudden_sales_spike", "large_inventory_adjustment"],
    )
    severity: AnomalySeverity = Field(description="Severity level of the anomaly")
    score: float = Field(ge=0.0, le=100.0, description="Anomaly score (0-100)")
    status: AnomalyStatus = Field(description="Current status of the anomaly")
    description: str | None = Field(default=None, description="Detailed description of the anomaly")
    detected_at: datetime = Field(description="Timestamp when anomaly was detected")
    resolved_at: datetime | None = Field(default=None, description="Timestamp when anomaly was resolved")


class AnomalyCreateRequest(BaseModel):
    """Request model for creating an anomaly (for testing/manual creation)."""

    inventory_id: UUID = Field(description="Associated inventory item identifier")
    anomaly_type: str = Field(description="Type of anomaly")
    severity: AnomalySeverity = Field(description="Severity level")
    score: float = Field(ge=0.0, le=100.0, description="Anomaly score")
    description: str | None = Field(default=None, description="Anomaly description")
    event_id: str | None = Field(default=None, description="Optional source event identifier")


class AnomalyFilterParams(BaseModel):
    """Query parameters for filtering anomalies."""

    inventory_id: UUID | None = Field(default=None, description="Filter by inventory ID")
    anomaly_type: str | None = Field(default=None, description="Filter by anomaly type")
    severity: AnomalySeverity | None = Field(default=None, description="Filter by severity")
    status: AnomalyStatus | None = Field(default=None, description="Filter by status")
    skip: int = Field(default=0, ge=0, description="Number of records to skip")
    limit: int = Field(default=20, ge=1, le=100, description="Maximum number of records to return")


class PaginatedAnomaliesResponse(BaseModel):
    """Paginated response for anomalies list."""

    total: int = Field(description="Total number of anomalies matching filters")
    count: int = Field(description="Number of anomalies in this page")
    skip: int = Field(description="Number of records skipped")
    limit: int = Field(description="Maximum number of records returned")
    items: list[AnomalyResponse] = Field(description="List of anomalies")


class AnomalyStatusUpdate(BaseModel):
    """Request model for updating anomaly status."""

    status: AnomalyStatus = Field(description="New status for the anomaly")
    resolved_at: datetime | None = Field(
        default=None,
        description="Timestamp when resolved (optional, set to now if not provided)",
    )


class AnomalyStatsResponse(BaseModel):
    """Statistics about detected anomalies."""

    total_anomalies: int = Field(description="Total number of anomalies")
    open_anomalies: int = Field(description="Number of open anomalies")
    investigating_anomalies: int = Field(description="Number of investigating anomalies")
    resolved_anomalies: int = Field(description="Number of resolved anomalies")
    critical_count: int = Field(description="Number of critical severity anomalies")
    high_count: int = Field(description="Number of high severity anomalies")
    medium_count: int = Field(description="Number of medium severity anomalies")
    low_count: int = Field(description="Number of low severity anomalies")
    anomaly_types_count: dict[str, int] = Field(description="Count by anomaly type")
