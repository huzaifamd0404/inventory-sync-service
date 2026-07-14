from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import select
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.database.models import ProcessedEvent, ProcessedEventStatus
from app.database.session import SessionLocal
from app.schemas.inventory import InventoryEvent
from app.services.exceptions import DuplicateEvent
from app.services.inventory_service import InventoryTransientError


class ProcessedEventService:
    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    def assert_not_processed(self, event_id: str) -> None:
        try:
            with self._session_factory() as session:
                existing = session.execute(
                    select(ProcessedEvent).where(ProcessedEvent.event_id == event_id)
                ).scalar_one_or_none()
                if existing is not None:
                    raise DuplicateEvent(f"event already processed: {event_id}")
        except DuplicateEvent:
            raise
        except (OperationalError, SQLAlchemyError) as exc:
            raise InventoryTransientError("failed to read processed event state") from exc

    def mark_processed(self, event: InventoryEvent) -> None:
        try:
            with self._session_factory() as session:
                with session.begin():
                    existing = session.execute(
                        select(ProcessedEvent).where(
                            ProcessedEvent.event_id == str(event.event_id)
                        )
                    ).scalar_one_or_none()
                    if existing is not None:
                        return

                    session.add(
                        ProcessedEvent(
                            event_id=str(event.event_id),
                            product_id=event.product_id,
                            store_id=event.store_id,
                            status=ProcessedEventStatus.PROCESSED,
                        )
                    )
        except (OperationalError, SQLAlchemyError) as exc:
            raise InventoryTransientError("failed to mark processed event") from exc


def get_processed_event_service() -> ProcessedEventService:
    return ProcessedEventService(session_factory=SessionLocal)
