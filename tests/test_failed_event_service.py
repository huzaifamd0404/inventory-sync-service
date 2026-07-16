from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.database.base import Base
from app.database.models import FailedEvent
from app.services.failed_event_service import FailedEventService


def make_session_factory() -> sessionmaker[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_record_failure_persists_failed_event() -> None:
    session_factory = make_session_factory()
    service = FailedEventService(session_factory=session_factory)

    service.record_failure(
        event_id="evt-1",
        source_topic="inventory_updates",
        source_partition=0,
        source_offset=99,
        payload={"event_id": "evt-1", "quantity": 5},
        failure_reason="transient_failure_after_retries",
        retry_count=3,
    )

    with session_factory() as session:
        rows = session.execute(select(FailedEvent)).scalars().all()

    assert len(rows) == 1
    assert rows[0].event_id == "evt-1"
    assert rows[0].retry_count == 3


def test_record_failure_is_idempotent_for_same_kafka_location() -> None:
    session_factory = make_session_factory()
    service = FailedEventService(session_factory=session_factory)

    service.record_failure(
        event_id="evt-2",
        source_topic="inventory_updates",
        source_partition=1,
        source_offset=100,
        payload={"event_id": "evt-2"},
        failure_reason="non_retryable_failure",
        retry_count=0,
    )
    service.record_failure(
        event_id="evt-2",
        source_topic="inventory_updates",
        source_partition=1,
        source_offset=100,
        payload={"event_id": "evt-2"},
        failure_reason="non_retryable_failure",
        retry_count=0,
    )

    with session_factory() as session:
        rows = session.execute(select(FailedEvent)).scalars().all()

    assert len(rows) == 1
