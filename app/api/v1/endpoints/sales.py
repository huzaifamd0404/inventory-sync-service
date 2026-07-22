from __future__ import annotations

import logging
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session

from app.database.session import get_db_session
from app.producer.sales_kafka_producer import KafkaSalesProducer, KafkaSalesPublishError
from app.schemas.common import ErrorResponse
from app.schemas.sales import (
    SalesEvent,
    SalesEventCreate,
    SalesEventPublishResponse,
    SalesSummaryResponse,
)
from app.services.sales_service import SalesService, get_sales_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/sales")


def get_sales_kafka_producer() -> KafkaSalesProducer:
    return KafkaSalesProducer.from_settings()


@router.post(
    "/events",
    response_model=SalesEventPublishResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Publish sales event",
    description=(
        "Accepts a sales transaction and publishes it to the `sales_events` Kafka topic "
        "for downstream processing by the sales consumer."
    ),
    responses={
        202: {
            "description": "Sales event accepted for asynchronous processing.",
            "content": {
                "application/json": {
                    "example": {
                        "event_id": "0e9f4d70-98a3-41f3-b9bc-7439f4ac0f57",
                        "sale_id": "ORDER-20260722-001",
                    }
                }
            },
        },
        422: {"description": "Validation failed for the request body.", "model": ErrorResponse},
        503: {"description": "Kafka publish unavailable.", "model": ErrorResponse},
        500: {"description": "Unexpected server error.", "model": ErrorResponse},
    },
)
async def publish_sales_event(
    payload: SalesEventCreate,
    request: Request,
    producer: KafkaSalesProducer = Depends(get_sales_kafka_producer),
) -> SalesEventPublishResponse:
    event = SalesEvent(event_id=uuid4(), **payload.model_dump())

    logger.info(
        "sales_event_received",
        extra={
            "event_id": str(event.event_id),
            "sale_id": event.sale_id,
            "product_id": event.product_id,
            "store_id": event.store_id,
            "quantity_sold": event.quantity_sold,
            "request_id": getattr(request.state, "request_id", "unknown"),
        },
    )

    try:
        await run_in_threadpool(producer.publish_sales_event, event)
    except KafkaSalesPublishError as exc:
        logger.exception(
            "sales_event_publish_unavailable",
            extra={
                "event_id": str(event.event_id),
                "sale_id": event.sale_id,
                "request_id": getattr(request.state, "request_id", "unknown"),
            },
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to publish sales event",
        ) from exc

    return SalesEventPublishResponse(event_id=event.event_id, sale_id=event.sale_id)


@router.get(
    "/{store_id}/{product_id}",
    response_model=SalesSummaryResponse,
    status_code=status.HTTP_200_OK,
    summary="Get sales summary",
    description=(
        "Returns the aggregated sales summary for a product/store combination, "
        "including individual transaction records ordered by most recent first."
    ),
    responses={
        200: {"description": "Sales summary returned successfully."},
        404: {
            "description": "No inventory record found for the given product/store.",
            "model": ErrorResponse,
        },
        500: {"description": "Unexpected server error.", "model": ErrorResponse},
    },
)
async def get_sales_summary(
    store_id: str,
    product_id: str,
    request: Request,
    db: Session = Depends(get_db_session),
    service: SalesService = Depends(get_sales_service),
) -> SalesSummaryResponse:
    logger.info(
        "sales_summary_requested",
        extra={
            "product_id": product_id,
            "store_id": store_id,
            "request_id": getattr(request.state, "request_id", "unknown"),
        },
    )

    summary = await run_in_threadpool(service.get_sales_summary, db, product_id, store_id)

    if summary is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No inventory found for product_id={product_id!r} store_id={store_id!r}",
        )

    return summary
