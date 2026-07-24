"""REST API endpoints for alert management."""
from __future__ import annotations

import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.orm import Session

from app.database.session import get_db_session
from app.schemas.alert import (
    AlertAcknowledgeRequest,
    AlertFilterParams,
    AlertResolveRequest,
    AlertResponse,
    AlertStatsResponse,
    AlertSuppressRequest,
    PaginatedAlertsResponse,
)
from app.schemas.common import ErrorResponse
from app.services.alert_service import (
    AlertNotFoundError,
    AlertService,
    AlertServiceError,
    AlertTransientError,
    get_alert_service,
)
from app.database.models import AlertSeverity, AlertStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/alerts")


@router.get(
    "",
    response_model=PaginatedAlertsResponse,
    status_code=status.HTTP_200_OK,
    summary="List alerts",
    description="List triggered alerts with optional filtering and pagination.",
    responses={
        200: {
            "description": "Successfully retrieved alerts.",
            "model": PaginatedAlertsResponse,
        },
        500: {"description": "Internal server error.", "model": ErrorResponse},
    },
)
async def list_alerts(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(20, ge=1, le=100, description="Maximum records to return"),
    inventory_id: UUID | None = Query(None, description="Filter by inventory ID"),
    anomaly_id: UUID | None = Query(None, description="Filter by anomaly ID"),
    severity: AlertSeverity | None = Query(None, description="Filter by severity"),
    status: AlertStatus | None = Query(None, description="Filter by status"),
    request: Request = None,
    alert_service: AlertService = Depends(get_alert_service),
) -> PaginatedAlertsResponse:
    """List alerts with optional filters.

    Query Parameters:
    - skip: Number of records to skip (default: 0)
    - limit: Maximum records to return (default: 20, max: 100)
    - inventory_id: Filter by inventory UUID
    - anomaly_id: Filter by anomaly UUID
    - severity: Filter by severity (high, critical)
    - status: Filter by status (triggered, acknowledged, resolved, suppressed)
    """
    try:
        result = alert_service.list_alerts(
            skip=skip,
            limit=limit,
            inventory_id=str(inventory_id) if inventory_id else None,
            anomaly_id=str(anomaly_id) if anomaly_id else None,
            severity=severity,
            status=status,
        )

        return PaginatedAlertsResponse(
            total=result.total,
            count=len(result.alerts),
            skip=skip,
            limit=limit,
            items=[AlertResponse.model_validate(a) for a in result.alerts],
        )

    except AlertTransientError as exc:
        logger.exception("list_alerts_transient_error")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service temporarily unavailable",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("list_alerts_unexpected_error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc


@router.get(
    "/{alert_id}",
    response_model=AlertResponse,
    status_code=status.HTTP_200_OK,
    summary="Get alert",
    description="Retrieve a specific alert by ID.",
    responses={
        200: {"description": "Alert found.", "model": AlertResponse},
        404: {"description": "Alert not found.", "model": ErrorResponse},
        500: {"description": "Internal server error.", "model": ErrorResponse},
    },
)
async def get_alert(
    alert_id: UUID,
    request: Request = None,
    alert_service: AlertService = Depends(get_alert_service),
) -> AlertResponse:
    """Get a specific alert by ID."""
    try:
        alert = alert_service.get_alert(str(alert_id))
        return AlertResponse.model_validate(alert)

    except AlertNotFoundError as exc:
        logger.info("alert_not_found", extra={"alert_id": str(alert_id)})
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert not found",
        ) from exc
    except AlertTransientError as exc:
        logger.exception("get_alert_transient_error")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service temporarily unavailable",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("get_alert_unexpected_error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc


@router.post(
    "/{alert_id}/acknowledge",
    response_model=AlertResponse,
    status_code=status.HTTP_200_OK,
    summary="Acknowledge alert",
    description="Acknowledge an alert to indicate it has been seen.",
    responses={
        200: {"description": "Alert acknowledged.", "model": AlertResponse},
        404: {"description": "Alert not found.", "model": ErrorResponse},
        500: {"description": "Internal server error.", "model": ErrorResponse},
    },
)
async def acknowledge_alert(
    alert_id: UUID,
    request_body: AlertAcknowledgeRequest,
    request: Request = None,
    alert_service: AlertService = Depends(get_alert_service),
) -> AlertResponse:
    """Acknowledge an alert."""
    try:
        alert = alert_service.acknowledge_alert(str(alert_id), request_body.acknowledged_by)
        return AlertResponse.model_validate(alert)

    except AlertNotFoundError as exc:
        logger.info("alert_not_found", extra={"alert_id": str(alert_id)})
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert not found",
        ) from exc
    except AlertTransientError as exc:
        logger.exception("acknowledge_alert_transient_error")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service temporarily unavailable",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("acknowledge_alert_unexpected_error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc


@router.post(
    "/{alert_id}/resolve",
    response_model=AlertResponse,
    status_code=status.HTTP_200_OK,
    summary="Resolve alert",
    description="Mark an alert as resolved.",
    responses={
        200: {"description": "Alert resolved.", "model": AlertResponse},
        404: {"description": "Alert not found.", "model": ErrorResponse},
        500: {"description": "Internal server error.", "model": ErrorResponse},
    },
)
async def resolve_alert(
    alert_id: UUID,
    request_body: AlertResolveRequest,
    request: Request = None,
    alert_service: AlertService = Depends(get_alert_service),
) -> AlertResponse:
    """Resolve an alert."""
    try:
        alert = alert_service.resolve_alert(str(alert_id))
        return AlertResponse.model_validate(alert)

    except AlertNotFoundError as exc:
        logger.info("alert_not_found", extra={"alert_id": str(alert_id)})
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert not found",
        ) from exc
    except AlertTransientError as exc:
        logger.exception("resolve_alert_transient_error")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service temporarily unavailable",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("resolve_alert_unexpected_error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc


@router.post(
    "/{alert_id}/suppress",
    response_model=AlertResponse,
    status_code=status.HTTP_200_OK,
    summary="Suppress alert",
    description="Suppress an alert until a specified time.",
    responses={
        200: {"description": "Alert suppressed.", "model": AlertResponse},
        404: {"description": "Alert not found.", "model": ErrorResponse},
        500: {"description": "Internal server error.", "model": ErrorResponse},
    },
)
async def suppress_alert(
    alert_id: UUID,
    request_body: AlertSuppressRequest,
    request: Request = None,
    alert_service: AlertService = Depends(get_alert_service),
) -> AlertResponse:
    """Suppress an alert."""
    try:
        alert = alert_service.suppress_alert(str(alert_id), request_body.suppressed_until)
        return AlertResponse.model_validate(alert)

    except AlertNotFoundError as exc:
        logger.info("alert_not_found", extra={"alert_id": str(alert_id)})
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert not found",
        ) from exc
    except AlertTransientError as exc:
        logger.exception("suppress_alert_transient_error")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service temporarily unavailable",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("suppress_alert_unexpected_error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc


@router.get(
    "/stats/summary",
    response_model=AlertStatsResponse,
    status_code=status.HTTP_200_OK,
    summary="Get alert statistics",
    description="Get statistics about alerts.",
    responses={
        200: {"description": "Statistics retrieved.", "model": AlertStatsResponse},
        500: {"description": "Internal server error.", "model": ErrorResponse},
    },
)
async def get_alert_stats(
    request: Request = None,
    alert_service: AlertService = Depends(get_alert_service),
) -> AlertStatsResponse:
    """Get alert statistics."""
    try:
        stats = alert_service.get_alert_stats()

        return AlertStatsResponse(
            total_alerts=stats.total,
            triggered_count=stats.triggered,
            acknowledged_count=stats.acknowledged,
            resolved_count=stats.resolved,
            suppressed_count=stats.suppressed,
            critical_count=stats.critical,
            high_count=stats.high,
            avg_ack_time_seconds=stats.avg_ack_time,
            avg_resolution_time_seconds=stats.avg_resolution_time,
        )

    except AlertTransientError as exc:
        logger.exception("get_alert_stats_transient_error")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service temporarily unavailable",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("get_alert_stats_unexpected_error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc
