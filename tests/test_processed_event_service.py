from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.database.base import Base
from app.database.models import ProcessedEvent
from app.schemas.inventory import InventoryEvent, InventoryOperation
from app.services.exceptions import DuplicateEvent
from app.services.processed_event_service import ProcessedEventService


def make_session_factory() -> sessionmaker[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def make_event(event_id: str) -> InventoryEvent:
    return InventoryEvent(
        event_id=UUID(event_id),
        product_id="SKU-99",
        store_id="STORE-99",
        operation=InventoryOperation.RESTOCK,
        quantity=7,
        timestamp=datetime.now(UTC),
    )


def test_processed_event_service_marks_event_as_processed() -> None:
    session_factory = make_session_factory()
    service = ProcessedEventService(session_factory=session_factory)
    event = make_event("719594f9-f49f-4afe-a506-d7860ad8c8c4")

    service.assert_not_processed(str(event.event_id))
    service.mark_processed(event)

    with session_factory() as session:
        records = session.execute(select(ProcessedEvent)).scalars().all()

    assert len(records) == 1
    assert records[0].event_id == str(event.event_id)


def test_processed_event_service_raises_duplicate_event_when_already_processed() -> None:
    session_factory = make_session_factory()
    service = ProcessedEventService(session_factory=session_factory)
    event = make_event("f7ef437d-e345-486f-a9dc-1b873b89028f")

    service.mark_processed(event)

    with pytest.raises(DuplicateEvent):
        service.assert_not_processed(str(event.event_id))
