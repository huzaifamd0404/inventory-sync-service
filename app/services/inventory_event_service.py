import logging

from fastapi.concurrency import run_in_threadpool

from app.producer.kafka_producer import InventoryEventPublisher, KafkaPublishError
from app.schemas.inventory import InventoryEvent

logger = logging.getLogger(__name__)


class InventoryEventServiceError(Exception):
    """Raised when inventory event processing fails."""


class InventoryEventService:
    def __init__(self, publisher: InventoryEventPublisher) -> None:
        self._publisher = publisher

    async def publish_event(self, event: InventoryEvent) -> None:
        try:
            await run_in_threadpool(self._publisher.publish_inventory_event, event)
        except KafkaPublishError as exc:
            logger.exception(
                "inventory_event_service_publish_failed",
                extra={"event_id": str(event.event_id)},
            )
            raise InventoryEventServiceError("failed to publish inventory event") from exc
