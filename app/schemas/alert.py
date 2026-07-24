"""Pydantic schemas for alert management API."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.database.models import AlertSeverity, AlertStatus


class AlertResponse(BaseModel):
    """Response model for a single alert."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(description="Unique alert identifier")
    anomaly_id: UUID = Field(description="Associated anomaly identifier")
    inventory_id: UUID = Field(description="Associated inventory item identifier")
    event_id: str | None = Field(default=None, description="Source event identifier")
    severity: AlertSeverity = Field(description="Alert severity level (HIGH or CRITICAL)")
    status: AlertStatus = Field(description="Current status of the alert")
    title: str = Field(description="Alert title/summary")
    description: str | None = Field(default=None, description="Detailed description of the alert")
    triggered_at: datetime = Field(description="Timestamp when alert was triggered")
    acknowledged_at: datetime | None = Field(
        default=None, description="Timestamp when alert was acknowledged"
    )
    acknowledged_by: str | None = Field(default=None, description="User who acknowledged the alert")
    resolved_at: datetime | None = Field(default=None, description="Timestamp when alert was resolved")
    suppressed_until: datetime | None = Field(
        default=None, description="Timestamp until which alert is suppressed"
    )


class AlertCreateRequest(BaseModel):
    """Request model for creating an alert."""

    anomaly_id: UUID = Field(description="Associated anomaly identifier")
    severity: AlertSeverity = Field(description="Alert severity level")
    title: str = Field(description="Alert title/summary")
    description: str | None = Field(default=None, description="Alert description")
    event_id: str | None = Field(default=None, description="Optional source event identifier")


class AlertAcknowledgeRequest(BaseModel):
    """Request model for acknowledging an alert."""

    acknowledged_by: str = Field(description="User acknowledging the alert")


class AlertResolveRequest(BaseModel):
    """Request model for resolving an alert."""

    resolved_by: str = Field(description="User resolving the alert")


class AlertSuppressRequest(BaseModel):
    """Request model for suppressing an alert."""

    suppressed_until: datetime = Field(description="Timestamp until which to suppress the alert")
    reason: str | None = Field(default=None, description="Reason for suppression")


class AlertFilterParams(BaseModel):
    """Query parameters for filtering alerts."""

    inventory_id: UUID | None = Field(default=None, description="Filter by inventory ID")
    anomaly_id: UUID | None = Field(default=None, description="Filter by anomaly ID")
    severity: AlertSeverity | None = Field(default=None, description="Filter by severity")
    status: AlertStatus | None = Field(default=None, description="Filter by status")
    skip: int = Field(default=0, ge=0, description="Number of records to skip")
    limit: int = Field(default=20, ge=1, le=100, description="Maximum number of records to return")


class PaginatedAlertsResponse(BaseModel):
    """Paginated response for alerts list."""

    total: int = Field(description="Total number of alerts matching filters")
    count: int = Field(description="Number of alerts in this page")
    skip: int = Field(description="Number of records skipped")
    limit: int = Field(description="Maximum number of records returned")
    items: list[AlertResponse] = Field(description="List of alerts")


class AlertStatsResponse(BaseModel):
    """Statistics about alerts."""

    total_alerts: int = Field(description="Total number of alerts")
    triggered_count: int = Field(description="Number of triggered alerts")
    acknowledged_count: int = Field(description="Number of acknowledged alerts")
    resolved_count: int = Field(description="Number of resolved alerts")
    suppressed_count: int = Field(description="Number of suppressed alerts")
    critical_count: int = Field(description="Number of critical severity alerts")
    high_count: int = Field(description="Number of high severity alerts")
    avg_ack_time_seconds: float | None = Field(
        description="Average time to acknowledge alerts (in seconds)"
    )
    avg_resolution_time_seconds: float | None = Field(
        description="Average time to resolve alerts (in seconds)"
    )
