import logging
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, status

from app.producer.kafka_producer import KafkaInventoryProducer
from app.schemas.inventory import InventoryEvent, InventoryEventCreate, InventoryEventPublishResponse
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
)
async def publish_inventory_event(
    payload: InventoryEventCreate,
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
        },
    )

    try:
        await service.publish_event(event)
    except InventoryEventServiceError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="inventory event publish unavailable",
        ) from exc

    return InventoryEventPublishResponse(event_id=event.event_id)
