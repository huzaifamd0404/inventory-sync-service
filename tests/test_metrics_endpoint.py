"""Tests for metrics endpoint."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database.base import Base
from app.main import app
from app.models.batch import EventBatch
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

    def pipeline(self, transaction: bool = False):
        return FakePipeline(self)


class FakePipeline:
    """Mock Redis pipeline."""

    def __init__(self, redis_client: FakeRedis) -> None:
        self._redis = redis_client
        self._commands: list[tuple] = []

    def set(self, key: str, value: str) -> "FakePipeline":
        self._commands.append(("set", key, value))
        return self

    def setex(self, key: str, ttl: int, value: str) -> "FakePipeline":
        self._commands.append(("setex", key, ttl, value))
        return self

    def delete(self, key: str) -> "FakePipeline":
        self._commands.append(("delete", key))
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
                results.append(1)
        return results


def make_session_factory() -> sessionmaker[Session]:
    """Create an in-memory SQLite session factory for testing."""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def make_event(
    operation: InventoryOperation = InventoryOperation.RESTOCK,
    quantity: int = 5,
) -> InventoryEvent:
    """Create a test inventory event."""
    return InventoryEvent(
        event_id=uuid4(),
        product_id="SKU-TEST",
        store_id="STORE-TEST",
        operation=operation,
        quantity=quantity,
        timestamp=datetime.now(UTC),
    )


class TestMetricsEndpoint:
    """Tests for the metrics endpoint."""

    def test_get_metrics_empty_state(self) -> None:
        """Test getting metrics when no batches have been processed."""
        client = TestClient(app)
        response = client.get("/api/v1/metrics")

        assert response.status_code == 200
        data = response.json()

        assert data["total_batches_processed"] == 0
        assert data["total_events_processed"] == 0
        assert data["total_successful_events"] == 0
        assert data["total_failed_events"] == 0
        assert data["total_duplicate_events"] == 0

    def test_get_metrics_schema_validation(self) -> None:
        """Test that metrics response matches expected schema."""
        client = TestClient(app)
        response = client.get("/api/v1/metrics")

        assert response.status_code == 200
        data = response.json()

        # Verify all required fields are present
        required_fields = {
            "total_batches_processed",
            "total_events_processed",
            "total_successful_events",
            "total_failed_events",
            "total_duplicate_events",
            "total_processing_time_ms",
            "min_batch_processing_time_ms",
            "max_batch_processing_time_ms",
            "avg_batch_processing_time_ms",
            "total_batch_failures",
            "total_partial_failures",
            "total_redis_pipeline_operations",
            "total_redis_pipeline_errors",
            "total_database_operations",
            "total_database_errors",
            "last_updated_at",
        }

        for field in required_fields:
            assert field in data, f"Missing field: {field}"

    def test_get_metrics_data_types(self) -> None:
        """Test that metrics response has correct data types."""
        client = TestClient(app)
        response = client.get("/api/v1/metrics")

        assert response.status_code == 200
        data = response.json()

        assert isinstance(data["total_batches_processed"], int)
        assert isinstance(data["total_events_processed"], int)
        assert isinstance(data["total_successful_events"], int)
        assert isinstance(data["total_failed_events"], int)
        assert isinstance(data["total_duplicate_events"], int)
        assert isinstance(data["total_processing_time_ms"], (int, float))
        assert isinstance(data["min_batch_processing_time_ms"], (int, float))
        assert isinstance(data["max_batch_processing_time_ms"], (int, float))
        assert isinstance(data["avg_batch_processing_time_ms"], (int, float))
        assert isinstance(data["total_batch_failures"], int)
        assert isinstance(data["total_partial_failures"], int)
        assert isinstance(data["total_redis_pipeline_operations"], int)
        assert isinstance(data["total_redis_pipeline_errors"], int)
        assert isinstance(data["total_database_operations"], int)
        assert isinstance(data["total_database_errors"], int)
        assert isinstance(data["last_updated_at"], str)

    def test_get_metrics_consistent_values(self) -> None:
        """Test that metrics values are consistent."""
        client = TestClient(app)
        response = client.get("/api/v1/metrics")

        assert response.status_code == 200
        data = response.json()

        # Verify constraints
        assert data["total_successful_events"] <= data["total_events_processed"]
        assert data["total_failed_events"] <= data["total_events_processed"]
        assert (
            data["total_successful_events"]
            + data["total_failed_events"]
            + data["total_duplicate_events"]
            <= data["total_events_processed"]
        )
        assert data["total_redis_pipeline_errors"] <= data["total_redis_pipeline_operations"]
        assert data["total_database_errors"] <= data["total_database_operations"]

    def test_get_metrics_min_max_averages(self) -> None:
        """Test that min/max/average metrics make sense."""
        client = TestClient(app)
        response = client.get("/api/v1/metrics")

        assert response.status_code == 200
        data = response.json()

        # When no batches processed, these should be 0 or inf
        if data["total_batches_processed"] == 0:
            assert data["min_batch_processing_time_ms"] == 0.0
            assert data["max_batch_processing_time_ms"] == 0.0
            assert data["avg_batch_processing_time_ms"] == 0.0
        else:
            # When batches exist, min should be <= max
            assert data["min_batch_processing_time_ms"] <= data["max_batch_processing_time_ms"]
            # Average should be between min and max
            assert data["min_batch_processing_time_ms"] <= data["avg_batch_processing_time_ms"]
            assert data["avg_batch_processing_time_ms"] <= data["max_batch_processing_time_ms"]

    def test_get_metrics_endpoint_is_publicly_documented(self) -> None:
        """Test that metrics endpoint appears in OpenAPI documentation."""
        client = TestClient(app)
        response = client.get("/openapi.json")

        assert response.status_code == 200
        openapi = response.json()

        # Check that metrics path is documented
        assert "/api/v1/metrics" in openapi["paths"]
        assert "get" in openapi["paths"]["/api/v1/metrics"]

    def test_get_metrics_response_has_200_status(self) -> None:
        """Test that metrics endpoint always returns 200 OK."""
        client = TestClient(app)

        # Call multiple times to ensure consistency
        for _ in range(5):
            response = client.get("/api/v1/metrics")
            assert response.status_code == 200
            assert response.headers["content-type"] == "application/json"

    def test_metrics_last_updated_timestamp_valid(self) -> None:
        """Test that last_updated_at timestamp is a valid ISO 8601 string."""
        client = TestClient(app)
        response = client.get("/api/v1/metrics")

        assert response.status_code == 200
        data = response.json()

        # Verify it's a valid ISO 8601 timestamp
        last_updated = data["last_updated_at"]
        assert isinstance(last_updated, str)
        # Should be parseable as ISO format
        datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
