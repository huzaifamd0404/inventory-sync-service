"""Integration tests for anomaly detection REST endpoints."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.database.models import (
    Anomaly,
    AnomalySeverity,
    AnomalyStatus,
    Inventory,
)
from app.main import app


@pytest.fixture
def client():
    """Provide FastAPI test client."""
    return TestClient(app)


class TestAnomaliesListEndpoint:
    """Test GET /api/v1/anomalies endpoint."""

    def test_list_anomalies_empty(self, client, session_factory):
        """Test listing anomalies when none exist."""
        response = client.get("/api/v1/anomalies")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 0
        assert data["count"] == 0
        assert len(data["items"]) == 0

    def test_list_anomalies_with_results(self, client, session_factory):
        """Test listing anomalies with results."""
        # Create test data
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-API",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            anomaly = Anomaly(
                inventory_id=inventory.id,
                anomaly_type="test_anomaly",
                severity=AnomalySeverity.HIGH,
                score=75.0,
                status=AnomalyStatus.OPEN,
                description="Test anomaly",
            )
            session.add(anomaly)
            session.commit()

        response = client.get("/api/v1/anomalies")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 1
        assert data["count"] == 1
        assert data["items"][0]["anomaly_type"] == "test_anomaly"
        assert data["items"][0]["severity"] == "high"

    def test_list_anomalies_with_pagination(self, client, session_factory):
        """Test listing anomalies with pagination."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-PAGE",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            for i in range(25):
                anomaly = Anomaly(
                    inventory_id=inventory.id,
                    anomaly_type=f"anomaly_{i}",
                    severity=AnomalySeverity.LOW,
                    score=50.0,
                    status=AnomalyStatus.OPEN,
                )
                session.add(anomaly)
            session.commit()

        # Get first page
        response = client.get("/api/v1/anomalies?skip=0&limit=10")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 25
        assert data["count"] == 10

        # Get second page
        response = client.get("/api/v1/anomalies?skip=10&limit=10")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["count"] == 10

    def test_list_anomalies_filter_by_severity(self, client, session_factory):
        """Test filtering anomalies by severity."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-FILTER-SEV",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            for severity in [AnomalySeverity.CRITICAL, AnomalySeverity.HIGH, AnomalySeverity.LOW]:
                anomaly = Anomaly(
                    inventory_id=inventory.id,
                    anomaly_type="test",
                    severity=severity,
                    score=60.0,
                    status=AnomalyStatus.OPEN,
                )
                session.add(anomaly)
            session.commit()

        response = client.get("/api/v1/anomalies?severity=critical")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["severity"] == "critical"


class TestAnomaliesGetEndpoint:
    """Test GET /api/v1/anomalies/{anomaly_id} endpoint."""

    def test_get_anomaly_success(self, client, session_factory):
        """Test retrieving a specific anomaly."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-GET-API",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            anomaly = Anomaly(
                inventory_id=inventory.id,
                anomaly_type="test",
                severity=AnomalySeverity.HIGH,
                score=80.0,
                status=AnomalyStatus.OPEN,
                description="Test anomaly",
            )
            session.add(anomaly)
            session.commit()
            anomaly_id = anomaly.id

        response = client.get(f"/api/v1/anomalies/{anomaly_id}")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert str(data["id"]) == str(anomaly_id)
        assert data["anomaly_type"] == "test"
        assert data["description"] == "Test anomaly"

    def test_get_anomaly_not_found(self, client):
        """Test retrieving non-existent anomaly."""
        response = client.get(f"/api/v1/anomalies/{uuid4()}")

        assert response.status_code == status.HTTP_404_NOT_FOUND


class TestAnomaliesByInventoryEndpoint:
    """Test GET /api/v1/anomalies/inventory/{inventory_id} endpoint."""

    def test_get_anomalies_by_inventory(self, client, session_factory):
        """Test retrieving anomalies for an inventory."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-BY-INV",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            for i in range(3):
                anomaly = Anomaly(
                    inventory_id=inventory.id,
                    anomaly_type=f"test_{i}",
                    severity=AnomalySeverity.LOW,
                    score=50.0,
                    status=AnomalyStatus.OPEN,
                )
                session.add(anomaly)
            session.commit()
            inventory_id = inventory.id

        response = client.get(f"/api/v1/anomalies/inventory/{inventory_id}")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 3
        assert all(a["inventory_id"] == str(inventory_id) for a in data)


class TestAnomalyStatusUpdateEndpoint:
    """Test PATCH /api/v1/anomalies/{anomaly_id}/status endpoint."""

    def test_update_anomaly_status_to_investigating(self, client, session_factory):
        """Test updating anomaly status to investigating."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-UPDATE-API",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            anomaly = Anomaly(
                inventory_id=inventory.id,
                anomaly_type="test",
                severity=AnomalySeverity.HIGH,
                score=80.0,
                status=AnomalyStatus.OPEN,
            )
            session.add(anomaly)
            session.commit()
            anomaly_id = anomaly.id

        response = client.patch(
            f"/api/v1/anomalies/{anomaly_id}/status",
            json={"status": "investigating"},
        )

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "investigating"

    def test_update_anomaly_status_to_resolved(self, client, session_factory):
        """Test updating anomaly status to resolved."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-RESOLVE-API",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            anomaly = Anomaly(
                inventory_id=inventory.id,
                anomaly_type="test",
                severity=AnomalySeverity.HIGH,
                score=80.0,
                status=AnomalyStatus.INVESTIGATING,
            )
            session.add(anomaly)
            session.commit()
            anomaly_id = anomaly.id

        response = client.patch(
            f"/api/v1/anomalies/{anomaly_id}/status",
            json={"status": "resolved"},
        )

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "resolved"
        assert data["resolved_at"] is not None

    def test_update_nonexistent_anomaly(self, client):
        """Test updating non-existent anomaly."""
        response = client.patch(
            f"/api/v1/anomalies/{uuid4()}/status",
            json={"status": "resolved"},
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND


class TestAnomalyStatsEndpoint:
    """Test GET /api/v1/anomalies/stats/summary endpoint."""

    def test_get_anomaly_stats(self, client, session_factory):
        """Test retrieving anomaly statistics."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-STATS-API",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            # Add various anomalies
            anomalies_data = [
                (AnomalyStatus.OPEN, AnomalySeverity.CRITICAL, "negative_inventory"),
                (AnomalyStatus.OPEN, AnomalySeverity.HIGH, "sudden_sales_spike"),
                (AnomalyStatus.INVESTIGATING, AnomalySeverity.MEDIUM, "large_adjustment"),
                (AnomalyStatus.RESOLVED, AnomalySeverity.LOW, "rapid_sales"),
            ]

            for status, severity, atype in anomalies_data:
                anomaly = Anomaly(
                    inventory_id=inventory.id,
                    anomaly_type=atype,
                    severity=severity,
                    score=60.0,
                    status=status,
                )
                session.add(anomaly)
            session.commit()

        response = client.get("/api/v1/anomalies/stats/summary")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total_anomalies"] == 4
        assert data["open_anomalies"] == 2
        assert data["investigating_anomalies"] == 1
        assert data["resolved_anomalies"] == 1
        assert data["critical_count"] == 1
        assert data["high_count"] == 1
        assert data["medium_count"] == 1
        assert data["low_count"] == 1
