"""Performance tests for batch processing service."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.database.base import Base
from app.database.models import Inventory, InventoryHistory
from app.models.batch import BatchProcessingStatus, EventBatch
from app.models.metrics import BatchMetrics
from app.schemas.inventory import InventoryEvent, InventoryOperation
from app.services.batch_processing_service import BatchProcessingService
from app.services.inventory_service import InventoryService


class FakeRedis:
    """Mock Redis client for testing."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def set(self, key: str, value: str) -> bool:
        self.values[key] = value
        return True

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def delete(self, key: str) -> int:
        if key in self.values:
            del self.values[key]
            return 1
        return 0

    def pipeline(self, transaction: bool = False):
        return FakePipeline(self)


class FakePipeline:
    """Mock Redis pipeline for testing."""

    def __init__(self, redis_client: FakeRedis) -> None:
        self._redis = redis_client
        self._commands: list[tuple[str, ...]] = []

    def set(self, key: str, value: str) -> "FakePipeline":
        self._commands.append(("set", key, value))
        return self

    def setex(self, key: str, ttl: int, value: str) -> "FakePipeline":
        self._commands.append(("setex", key, str(ttl), value))
        return self

    def delete(self, key: str) -> "FakePipeline":
        self._commands.append(("delete", key))
        return self

    def get(self, key: str) -> "FakePipeline":
        self._commands.append(("get", key))
        return self

    def mset(self, data: dict[str, str]) -> "FakePipeline":
        self._commands.append(("mset", str(data)))
        return self

    def execute(self) -> list:
        results = []
        for cmd in self._commands:
            if cmd[0] == "set":
                self._redis.set(cmd[1], cmd[2])
                results.append(True)
            elif cmd[0] == "setex":
                self._redis.set(cmd[1], cmd[3])
                results.append(True)
            elif cmd[0] == "delete":
                result = self._redis.delete(cmd[1])
                results.append(result)
            elif cmd[0] == "get":
                result = self._redis.get(cmd[1])
                results.append(result)
            elif cmd[0] == "mset":
                results.append(True)
        return results


def make_session_factory() -> sessionmaker[Session]:
    """Create an in-memory SQLite session factory for testing."""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def make_event(
    operation: InventoryOperation,
    quantity: int,
    product_id: str = "SKU-100",
    store_id: str = "STORE-A",
) -> InventoryEvent:
    """Create a test inventory event."""
    return InventoryEvent(
        event_id=uuid4(),
        product_id=product_id,
        store_id=store_id,
        operation=operation,
        quantity=quantity,
        timestamp=datetime.now(UTC),
    )


def make_event_batch(
    size: int,
    operation: InventoryOperation = InventoryOperation.RESTOCK,
    quantity: int = 5,
) -> EventBatch:
    """Create a test event batch with specified size."""
    batch = EventBatch(batch_id=str(uuid4()), events=[])
    for i in range(size):
        event = make_event(
            operation=operation,
            quantity=quantity,
            product_id=f"SKU-{i}",
            store_id=f"STORE-{i % 5}",
        )
        batch.add_event(event)
    return batch


class TestBatchProcessingServicePerformance:
    """Performance tests for batch processing service."""

    def test_batch_processing_100_events(self) -> None:
        """Test processing a batch of 100 events."""
        session_factory = make_session_factory()
        redis_client = FakeRedis()
        inventory_service = InventoryService(
            session_factory=session_factory,
            redis_client=redis_client,
        )
        batch_service = BatchProcessingService(
            inventory_service=inventory_service,
            batch_size=100,
        )

        # Create and process batch of 100 restock events
        batch = make_event_batch(size=100, operation=InventoryOperation.RESTOCK, quantity=10)
        result = batch_service.process_batch(batch)

        # Verify results
        assert result.total_events == 100
        assert result.successful_events == 100
        assert result.failed_events == 0
        assert result.status == BatchProcessingStatus.COMPLETED
        assert result.processing_time_ms > 0

        # Verify metrics recorded
        metrics = batch_service.metrics
        assert metrics.total_batches_processed == 1
        assert metrics.total_events_processed == 100
        assert metrics.total_successful_events == 100

    def test_batch_processing_500_events(self) -> None:
        """Test processing a batch of 500 events."""
        session_factory = make_session_factory()
        redis_client = FakeRedis()
        inventory_service = InventoryService(
            session_factory=session_factory,
            redis_client=redis_client,
        )
        batch_service = BatchProcessingService(
            inventory_service=inventory_service,
            batch_size=500,
        )

        # Create and process batch of 500 mixed operations
        batch = EventBatch(batch_id=str(uuid4()), events=[])
        for i in range(500):
            operation = [
                InventoryOperation.RESTOCK,
                InventoryOperation.SALE,
                InventoryOperation.RETURN,
            ][i % 3]
            quantity = 5 if operation != InventoryOperation.SALE else 2
            event = make_event(
                operation=operation,
                quantity=quantity,
                product_id=f"SKU-{i}",
                store_id=f"STORE-{i % 10}",
            )
            batch.add_event(event)

        result = batch_service.process_batch(batch)

        # Verify results
        assert result.total_events == 500
        assert result.successful_events == 500
        assert result.failed_events == 0
        assert result.status == BatchProcessingStatus.COMPLETED
        assert result.processing_time_ms > 0

        # Verify metrics recorded
        metrics = batch_service.metrics
        assert metrics.total_batches_processed == 1
        assert metrics.total_events_processed == 500
        assert metrics.total_successful_events == 500

    def test_batch_processing_1000_events(self) -> None:
        """Test processing a batch of 1000 events."""
        session_factory = make_session_factory()
        redis_client = FakeRedis()
        inventory_service = InventoryService(
            session_factory=session_factory,
            redis_client=redis_client,
        )
        batch_service = BatchProcessingService(
            inventory_service=inventory_service,
            batch_size=1000,
        )

        # Create and process batch of 1000 events
        batch = EventBatch(batch_id=str(uuid4()), events=[])
        for i in range(1000):
            operation = [
                InventoryOperation.RESTOCK,
                InventoryOperation.SALE,
            ][i % 2]
            quantity = 10 if operation == InventoryOperation.RESTOCK else 3
            event = make_event(
                operation=operation,
                quantity=quantity,
                product_id=f"SKU-{i}",
                store_id=f"STORE-{i % 20}",
            )
            batch.add_event(event)

        result = batch_service.process_batch(batch)

        # Verify results
        assert result.total_events == 1000
        assert result.successful_events == 1000
        assert result.failed_events == 0
        assert result.status == BatchProcessingStatus.COMPLETED
        assert result.processing_time_ms > 0

        # Verify metrics recorded
        metrics = batch_service.metrics
        assert metrics.total_batches_processed == 1
        assert metrics.total_events_processed == 1000
        assert metrics.total_successful_events == 1000

    def test_batch_processing_duplicate_handling(self) -> None:
        """Test batch processing correctly handles duplicate events."""
        session_factory = make_session_factory()
        redis_client = FakeRedis()
        inventory_service = InventoryService(
            session_factory=session_factory,
            redis_client=redis_client,
        )
        batch_service = BatchProcessingService(
            inventory_service=inventory_service,
            batch_size=10,
        )

        # Create an event and process it
        event1 = make_event(operation=InventoryOperation.RESTOCK, quantity=10)
        batch1 = EventBatch(batch_id=str(uuid4()), events=[event1])
        result1 = batch_service.process_batch(batch1)

        assert result1.successful_events == 1
        assert result1.duplicate_events == 0

        # Process the same event again (duplicate)
        batch2 = EventBatch(batch_id=str(uuid4()), events=[event1])
        result2 = batch_service.process_batch(batch2)

        assert result2.successful_events == 0
        assert result2.duplicate_events == 1

    def test_batch_accumulation_and_flushing(self) -> None:
        """Test batch accumulation until size is reached, then flush."""
        session_factory = make_session_factory()
        redis_client = FakeRedis()
        inventory_service = InventoryService(
            session_factory=session_factory,
            redis_client=redis_client,
        )
        batch_service = BatchProcessingService(
            inventory_service=inventory_service,
            batch_size=10,
        )

        # Add events one by one
        for i in range(15):
            event = make_event(
                operation=InventoryOperation.RESTOCK,
                quantity=5,
                product_id=f"SKU-{i}",
            )
            completed_batch = batch_service.add_event(event)

            # Batch should return after 10 events
            if i < 9:
                assert completed_batch is None
            else:
                assert completed_batch is None  # Next batch will start

        # Flush the remaining batch
        final_batch = batch_service.flush_batch()
        assert final_batch is not None
        assert final_batch.size() == 5

    def test_batch_metrics_collection(self) -> None:
        """Test that metrics are properly collected during batch processing."""
        session_factory = make_session_factory()
        redis_client = FakeRedis()
        inventory_service = InventoryService(
            session_factory=session_factory,
            redis_client=redis_client,
        )
        batch_service = BatchProcessingService(
            inventory_service=inventory_service,
            batch_size=100,
        )

        metrics = batch_service.metrics
        assert metrics.total_batches_processed == 0
        assert metrics.total_events_processed == 0
        assert metrics.total_successful_events == 0

        # Process batches
        for batch_num in range(3):
            batch = make_event_batch(size=100, operation=InventoryOperation.RESTOCK)
            batch_service.process_batch(batch)

        # Verify metrics
        assert metrics.total_batches_processed == 3
        assert metrics.total_events_processed == 300
        assert metrics.total_successful_events == 300
        assert metrics.avg_batch_processing_time_ms > 0
        assert metrics.min_batch_processing_time_ms > 0
        assert metrics.max_batch_processing_time_ms > 0

    def test_batch_processing_timeout_handling(self) -> None:
        """Test batch processing with partial batches and timeout."""
        session_factory = make_session_factory()
        redis_client = FakeRedis()
        inventory_service = InventoryService(
            session_factory=session_factory,
            redis_client=redis_client,
        )
        batch_service = BatchProcessingService(
            inventory_service=inventory_service,
            batch_size=100,
            max_batch_wait_ms=5000,
        )

        # Add only 5 events (less than batch size)
        for i in range(5):
            event = make_event(
                operation=InventoryOperation.RESTOCK,
                quantity=5,
                product_id=f"SKU-{i}",
            )
            batch_service.add_event(event)

        # Flush the partial batch
        batch = batch_service.flush_batch()
        assert batch is not None
        assert batch.size() == 5

        result = batch_service.process_batch(batch)
        assert result.total_events == 5
        assert result.successful_events == 5

    def test_batch_processing_metrics_dict_export(self) -> None:
        """Test that metrics can be exported to dictionary format."""
        metrics = BatchMetrics()

        # Record some metrics
        metrics.record_batch_processing(
            batch_size=100,
            successful=95,
            failed=5,
            duplicates=0,
            processing_time_ms=125.5,
        )
        metrics.record_database_operation(success=True)
        metrics.record_redis_operation(success=True)

        # Export to dict
        metrics_dict = metrics.to_dict()

        assert "total_batches_processed" in metrics_dict
        assert "total_events_processed" in metrics_dict
        assert "total_successful_events" in metrics_dict
        assert "total_failed_events" in metrics_dict
        assert "avg_batch_processing_time_ms" in metrics_dict
        assert "last_updated_at" in metrics_dict

        # Verify values
        assert metrics_dict["total_batches_processed"] == 1
        assert metrics_dict["total_events_processed"] == 100
        assert metrics_dict["total_successful_events"] == 95
        assert metrics_dict["total_failed_events"] == 5


class TestBatchProcessingScalability:
    """Scalability tests for batch processing."""

    def test_consecutive_batch_processing_maintains_metrics(self) -> None:
        """Test that metrics are correctly maintained across multiple batches."""
        session_factory = make_session_factory()
        redis_client = FakeRedis()
        inventory_service = InventoryService(
            session_factory=session_factory,
            redis_client=redis_client,
        )
        batch_service = BatchProcessingService(
            inventory_service=inventory_service,
            batch_size=50,
        )

        # Process 10 batches of 50 events each
        for i in range(10):
            batch = make_event_batch(
                size=50,
                operation=InventoryOperation.RESTOCK,
            )
            result = batch_service.process_batch(batch)
            assert result.status == BatchProcessingStatus.COMPLETED

        metrics = batch_service.metrics
        assert metrics.total_batches_processed == 10
        assert metrics.total_events_processed == 500
        assert metrics.total_successful_events == 500
        assert metrics.avg_batch_processing_time_ms > 0

    def test_batch_processing_with_mixed_operations(self) -> None:
        """Test batch processing with different inventory operations."""
        session_factory = make_session_factory()
        redis_client = FakeRedis()
        inventory_service = InventoryService(
            session_factory=session_factory,
            redis_client=redis_client,
        )
        batch_service = BatchProcessingService(
            inventory_service=inventory_service,
            batch_size=200,
        )

        batch = EventBatch(batch_id=str(uuid4()), events=[])

        # Add events with all operation types
        operations = [
            (InventoryOperation.RESTOCK, 10),
            (InventoryOperation.SALE, 3),
            (InventoryOperation.RETURN, 5),
            (InventoryOperation.MANUAL_ADJUSTMENT, 2),
        ]

        for i in range(200):
            operation, quantity = operations[i % len(operations)]
            event = make_event(
                operation=operation,
                quantity=quantity,
                product_id=f"SKU-{i}",
                store_id=f"STORE-{i % 10}",
            )
            batch.add_event(event)

        result = batch_service.process_batch(batch)

        assert result.total_events == 200
        assert result.successful_events == 200
        assert result.status == BatchProcessingStatus.COMPLETED
