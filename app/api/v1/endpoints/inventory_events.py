import logging
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.producer.kafka_producer import KafkaInventoryProducer
from app.schemas.common import ErrorResponse
from app.schemas.inventory import (
    InventoryEvent,
    InventoryEventCreate,
    InventoryEventPublishResponse,
)
from app.services.inventory_event_service import InventoryEventService, InventoryEventServiceError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/inventory")


def get_inventory_event_service() -> InventoryEventService:
    producer = KafkaInventoryProducer.from_settings()
    return InventoryEventService(publisher=producer)


@router.post(
    "/events",
    response_model=InventoryEventPublishResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Publish inventory event",
    description=(
        "Accepts an inventory change request and publishes it to Kafka for downstream "
        "processing by the inventory consumer."
    ),
    responses={
        202: {
            "description": "Event accepted for asynchronous processing.",
            "content": {
                "application/json": {
                    "example": {"event_id": "0e9f4d70-98a3-41f3-b9bc-7439f4ac0f57"}
                }
            },
        },
        422: {
            "description": "Validation failed for the request body.",
            "model": ErrorResponse,
        },
        503: {
            "description": "Kafka publish unavailable.",
            "model": ErrorResponse,
        },
        500: {
            "description": "Unexpected server error.",
            "model": ErrorResponse,
        },
    },
)
async def publish_inventory_event(
    payload: InventoryEventCreate,
    request: Request,
    service: InventoryEventService = Depends(get_inventory_event_service),
) -> InventoryEventPublishResponse:
    event = InventoryEvent(event_id=uuid4(), **payload.model_dump())

    logger.info(
        "inventory_event_received",
        extra={
            "event_id": str(event.event_id),
            "product_id": event.product_id,
            "store_id": event.store_id,
            "operation": event.operation.value,
            "request_id": getattr(request.state, "request_id", "unknown"),
        },
    )

    try:
        await service.publish_event(event)
    except InventoryEventServiceError as exc:
        logger.exception(
            "inventory_event_publish_unavailable",
            extra={
                "event_id": str(event.event_id),
                "product_id": event.product_id,
                "store_id": event.store_id,
                "request_id": getattr(request.state, "request_id", "unknown"),
            },
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to publish inventory event",
        ) from exc

    return InventoryEventPublishResponse(event_id=event.event_id)
