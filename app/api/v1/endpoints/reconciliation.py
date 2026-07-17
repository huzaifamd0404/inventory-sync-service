from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.database.session import SessionLocal
from app.schemas.common import ErrorResponse
from app.schemas.reconciliation import ReconciliationResponse
from app.services.reconciliation_service import (
    ReconciliationService,
    ReconciliationTransientError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/reconciliation")


def get_reconciliation_service() -> ReconciliationService:
    return ReconciliationService(session_factory=SessionLocal)


@router.get(
    "/{store_id}/{product_id}",
    response_model=ReconciliationResponse,
    status_code=status.HTTP_200_OK,
    summary="Reconcile inventory for a store/product pair",
    description=(
        "Derives the expected inventory quantity from the full audit history, "
        "compares it with the current snapshot, and returns the reconciliation result. "
        "A new record is persisted only when the status or difference changes."
    ),
    responses={
        200: {
            "description": "Reconciliation result for the requested store/product pair.",
            "content": {
                "application/json": {
                    "example": {
                        "store_id": "STORE-NYC",
                        "product_id": "SKU-100",
                        "expected_quantity": 42,
                        "actual_quantity": 40,
                        "difference": -2,
                        "status": "mismatch",
                        "reconciled_at": "2026-07-17T10:00:00Z",
                    }
                }
            },
        },
        503: {
            "description": "Transient database error prevented reconciliation.",
            "model": ErrorResponse,
        },
        500: {
            "description": "Unexpected server error.",
            "model": ErrorResponse,
        },
    },
)
def get_reconciliation(
    store_id: str,
    product_id: str,
    request: Request,
    service: ReconciliationService = Depends(get_reconciliation_service),
) -> ReconciliationResponse:
    request_id: str = getattr(request.state, "request_id", "unknown")

    logger.info(
        "reconciliation_request_received",
        extra={
            "store_id": store_id,
            "product_id": product_id,
            "request_id": request_id,
        },
    )

    try:
        result = service.reconcile(store_id=store_id, product_id=product_id)
    except ReconciliationTransientError as exc:
        logger.exception(
            "reconciliation_service_unavailable",
            extra={
                "store_id": store_id,
                "product_id": product_id,
                "request_id": request_id,
            },
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="reconciliation_service_unavailable",
        ) from exc

    logger.info(
        "reconciliation_request_completed",
        extra={
            "store_id": store_id,
            "product_id": product_id,
            "status": result.status.value,
            "difference": result.difference,
            "request_id": request_id,
        },
    )

    return ReconciliationResponse(
        store_id=result.store_id,
        product_id=result.product_id,
        expected_quantity=result.expected_quantity,
        actual_quantity=result.actual_quantity,
        difference=result.difference,
        status=result.status,
        reconciled_at=result.reconciled_at,
    )
