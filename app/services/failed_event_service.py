from __future__ import annotations

import json
from collections.abc import Callable

from sqlalchemy import select
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.database.models import FailedEvent
from app.database.session import SessionLocal
from app.services.inventory_service import InventoryTransientError


class FailedEventService:
    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    def record_failure(
        self,
        *,
        event_id: str | None,
        source_topic: str,
        source_partition: int,
        source_offset: int,
        payload: object,
        failure_reason: str,
        retry_count: int,
    ) -> FailedEvent:
        try:
            payload_text = json.dumps(payload, default=str, separators=(",", ":"))
            with self._session_factory() as session:
                with session.begin():
                    existing = session.execute(
                        select(FailedEvent)
                        .where(FailedEvent.source_topic == source_topic)
                        .where(FailedEvent.source_partition == source_partition)
                        .where(FailedEvent.source_offset == source_offset)
                    ).scalar_one_or_none()

                    if existing is not None:
                        return existing

                    failed_event = FailedEvent(
                        event_id=event_id,
                        source_topic=source_topic,
                        source_partition=source_partition,
                        source_offset=source_offset,
                        payload=payload_text,
                        failure_reason=failure_reason,
                        retry_count=retry_count,
                    )
                    session.add(failed_event)
                    session.flush()
                    return failed_event
        except (OperationalError, SQLAlchemyError, TypeError, ValueError) as exc:
            raise InventoryTransientError("failed to persist failed event") from exc


def get_failed_event_service() -> FailedEventService:
    return FailedEventService(session_factory=SessionLocal)
