"""REST API endpoints for anomaly detection and management."""
from __future__ import annotations

import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.orm import Session

from app.database.session import get_db_session
from app.schemas.anomaly import (
    AnomalyFilterParams,
    AnomalyResponse,
    AnomalyStatusUpdate,
    AnomalyStatsResponse,
    PaginatedAnomaliesResponse,
)
from app.schemas.common import ErrorResponse
from app.services.anomaly_service import (
    AnomalyNotFoundError,
    AnomalyService,
    AnomalyServiceError,
    AnomalyTransientError,
    get_anomaly_service,
)
from app.database.models import AnomalySeverity, AnomalyStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/anomalies")


@router.get(
    "",
    response_model=PaginatedAnomaliesResponse,
    status_code=status.HTTP_200_OK,
    summary="List anomalies",
    description="List detected anomalies with optional filtering and pagination.",
    responses={
        200: {
            "description": "Successfully retrieved anomalies.",
            "model": PaginatedAnomaliesResponse,
        },
        500: {"description": "Internal server error.", "model": ErrorResponse},
    },
)
async def list_anomalies(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(20, ge=1, le=100, description="Maximum records to return"),
    inventory_id: UUID | None = Query(None, description="Filter by inventory ID"),
    anomaly_type: str | None = Query(None, description="Filter by anomaly type"),
    severity: AnomalySeverity | None = Query(None, description="Filter by severity"),
    status: AnomalyStatus | None = Query(None, description="Filter by status"),
    request: Request = None,
    anomaly_service: AnomalyService = Depends(get_anomaly_service),
) -> PaginatedAnomaliesResponse:
    """List anomalies with optional filters.

    Query Parameters:
    - skip: Number of records to skip (default: 0)
    - limit: Maximum records to return (default: 20, max: 100)
    - inventory_id: Filter by inventory UUID
    - anomaly_type: Filter by type (e.g., negative_inventory, sudden_sales_spike)
    - severity: Filter by severity (low, medium, high, critical)
    - status: Filter by status (open, investigating, resolved)
    """
    try:
        result = anomaly_service.list_anomalies(
            skip=skip,
            limit=limit,
            inventory_id=str(inventory_id) if inventory_id else None,
            anomaly_type=anomaly_type,
            severity=severity,
            status=status,
        )

        return PaginatedAnomaliesResponse(
            total=result.total,
            count=len(result.anomalies),
            skip=skip,
            limit=limit,
            items=[AnomalyResponse.model_validate(a) for a in result.anomalies],
        )

    except AnomalyTransientError as exc:
        logger.exception("list_anomalies_transient_error")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service temporarily unavailable",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("list_anomalies_unexpected_error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc


@router.get(
    "/{anomaly_id}",
    response_model=AnomalyResponse,
    status_code=status.HTTP_200_OK,
    summary="Get anomaly",
    description="Retrieve a specific anomaly by ID.",
    responses={
        200: {"description": "Successfully retrieved anomaly.", "model": AnomalyResponse},
        404: {"description": "Anomaly not found.", "model": ErrorResponse},
        500: {"description": "Internal server error.", "model": ErrorResponse},
    },
)
async def get_anomaly(
    anomaly_id: UUID,
    request: Request = None,
    anomaly_service: AnomalyService = Depends(get_anomaly_service),
) -> AnomalyResponse:
    """Retrieve a specific anomaly by UUID."""
    try:
        anomaly = anomaly_service.get_anomaly(str(anomaly_id))
        return AnomalyResponse.model_validate(anomaly)

    except AnomalyNotFoundError as exc:
        logger.info("anomaly_not_found", extra={"anomaly_id": str(anomaly_id)})
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anomaly {anomaly_id} not found",
        ) from exc
    except AnomalyTransientError as exc:
        logger.exception("get_anomaly_transient_error", extra={"anomaly_id": str(anomaly_id)})
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service temporarily unavailable",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("get_anomaly_unexpected_error", extra={"anomaly_id": str(anomaly_id)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc


@router.get(
    "/inventory/{inventory_id}",
    response_model=list[AnomalyResponse],
    status_code=status.HTTP_200_OK,
    summary="Get anomalies by inventory",
    description="Get all anomalies for a specific inventory item.",
    responses={
        200: {
            "description": "Successfully retrieved anomalies.",
            "model": list[AnomalyResponse],
        },
        500: {"description": "Internal server error.", "model": ErrorResponse},
    },
)
async def get_anomalies_by_inventory(
    inventory_id: UUID,
    status: AnomalyStatus | None = Query(None, description="Filter by status"),
    request: Request = None,
    anomaly_service: AnomalyService = Depends(get_anomaly_service),
) -> list[AnomalyResponse]:
    """Get all anomalies for a specific inventory.

    Path Parameters:
    - inventory_id: UUID of the inventory item

    Query Parameters:
    - status: Optional filter by status (open, investigating, resolved)
    """
    try:
        anomalies = anomaly_service.get_anomalies_by_inventory(str(inventory_id), status)
        return [AnomalyResponse.model_validate(a) for a in anomalies]

    except AnomalyTransientError as exc:
        logger.exception(
            "get_anomalies_by_inventory_transient_error",
            extra={"inventory_id": str(inventory_id)},
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service temporarily unavailable",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "get_anomalies_by_inventory_unexpected_error",
            extra={"inventory_id": str(inventory_id)},
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc


@router.patch(
    "/{anomaly_id}/status",
    response_model=AnomalyResponse,
    status_code=status.HTTP_200_OK,
    summary="Update anomaly status",
    description="Update the status of an anomaly.",
    responses={
        200: {"description": "Successfully updated anomaly status.", "model": AnomalyResponse},
        404: {"description": "Anomaly not found.", "model": ErrorResponse},
        500: {"description": "Internal server error.", "model": ErrorResponse},
    },
)
async def update_anomaly_status(
    anomaly_id: UUID,
    update_request: AnomalyStatusUpdate,
    request: Request = None,
    anomaly_service: AnomalyService = Depends(get_anomaly_service),
) -> AnomalyResponse:
    """Update the status of an anomaly.

    Allows marking anomalies as investigating or resolved.
    """
    try:
        anomaly = anomaly_service.update_anomaly_status(
            str(anomaly_id),
            update_request.status,
            update_request.resolved_at,
        )
        return AnomalyResponse.model_validate(anomaly)

    except AnomalyNotFoundError as exc:
        logger.info("anomaly_not_found", extra={"anomaly_id": str(anomaly_id)})
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anomaly {anomaly_id} not found",
        ) from exc
    except AnomalyTransientError as exc:
        logger.exception(
            "update_anomaly_status_transient_error",
            extra={"anomaly_id": str(anomaly_id)},
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service temporarily unavailable",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "update_anomaly_status_unexpected_error",
            extra={"anomaly_id": str(anomaly_id)},
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc


@router.get(
    "/stats/summary",
    response_model=AnomalyStatsResponse,
    status_code=status.HTTP_200_OK,
    summary="Get anomaly statistics",
    description="Get summary statistics about detected anomalies.",
    responses={
        200: {
            "description": "Successfully retrieved statistics.",
            "model": AnomalyStatsResponse,
        },
        500: {"description": "Internal server error.", "model": ErrorResponse},
    },
)
async def get_anomaly_stats(
    request: Request = None,
    anomaly_service: AnomalyService = Depends(get_anomaly_service),
) -> AnomalyStatsResponse:
    """Get summary statistics about anomalies.

    Returns counts by status, severity, and type.
    """
    try:
        stats = anomaly_service.get_stats()
        return AnomalyStatsResponse(**stats)

    except AnomalyTransientError as exc:
        logger.exception("get_anomaly_stats_transient_error")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service temporarily unavailable",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("get_anomaly_stats_unexpected_error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc
