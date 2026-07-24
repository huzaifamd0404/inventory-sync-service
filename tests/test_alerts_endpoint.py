"""Integration tests for alert management REST endpoints."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.database.models import (
    Alert,
    AlertSeverity,
    AlertStatus,
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


class TestAlertsListEndpoint:
    """Test GET /api/v1/alerts endpoint."""

    def test_list_alerts_empty(self, client, session_factory):
        """Test listing alerts when none exist."""
        response = client.get("/api/v1/alerts")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 0
        assert data["count"] == 0
        assert len(data["items"]) == 0

    def test_list_alerts_with_results(self, client, session_factory):
        """Test listing alerts with results."""
        # Create test data
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-ALERT-API",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            anomaly = Anomaly(
                inventory_id=inventory.id,
                anomaly_type="test_anomaly",
                severity=AnomalySeverity.CRITICAL,
                score=95.0,
                status=AnomalyStatus.OPEN,
            )
            session.add(anomaly)
            session.commit()

            alert = Alert(
                anomaly_id=anomaly.id,
                inventory_id=inventory.id,
                severity=AlertSeverity.CRITICAL,
                status=AlertStatus.TRIGGERED,
                title="Test Alert",
                description="Test alert",
            )
            session.add(alert)
            session.commit()

        response = client.get("/api/v1/alerts")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 1
        assert data["count"] == 1
        assert data["items"][0]["title"] == "Test Alert"
        assert data["items"][0]["severity"] == "critical"
        assert data["items"][0]["status"] == "triggered"

    def test_list_alerts_with_pagination(self, client, session_factory):
        """Test listing alerts with pagination."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-ALERT-PAGE",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            for i in range(25):
                anomaly = Anomaly(
                    inventory_id=inventory.id,
                    anomaly_type=f"anomaly_{i}",
                    severity=AnomalySeverity.HIGH,
                    score=75.0,
                    status=AnomalyStatus.OPEN,
                )
                session.add(anomaly)
            session.commit()

            for anomaly in session.query(Anomaly).all():
                alert = Alert(
                    anomaly_id=anomaly.id,
                    inventory_id=inventory.id,
                    severity=AlertSeverity.HIGH,
                    status=AlertStatus.TRIGGERED,
                    title="Test Alert",
                )
                session.add(alert)
            session.commit()

        # Get first page
        response = client.get("/api/v1/alerts?skip=0&limit=10")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 25
        assert data["count"] == 10

        # Get second page
        response = client.get("/api/v1/alerts?skip=10&limit=10")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 25
        assert data["count"] == 10

    def test_list_alerts_filter_by_severity(self, client, session_factory):
        """Test filtering alerts by severity."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-ALERT-FILTER",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            # Create anomalies of different severities
            for severity, count in [(AnomalySeverity.CRITICAL, 3), (AnomalySeverity.HIGH, 2)]:
                for i in range(count):
                    anomaly = Anomaly(
                        inventory_id=inventory.id,
                        anomaly_type=f"anomaly_{severity.value}_{i}",
                        severity=severity,
                        score=80.0,
                        status=AnomalyStatus.OPEN,
                    )
                    session.add(anomaly)
            session.commit()

            for anomaly in session.query(Anomaly).all():
                alert_severity = (
                    AlertSeverity.CRITICAL
                    if anomaly.severity == AnomalySeverity.CRITICAL
                    else AlertSeverity.HIGH
                )
                alert = Alert(
                    anomaly_id=anomaly.id,
                    inventory_id=inventory.id,
                    severity=alert_severity,
                    status=AlertStatus.TRIGGERED,
                    title="Test Alert",
                )
                session.add(alert)
            session.commit()

        # Filter by CRITICAL
        response = client.get("/api/v1/alerts?severity=critical")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 3
        assert all(item["severity"] == "critical" for item in data["items"])

        # Filter by HIGH
        response = client.get("/api/v1/alerts?severity=high")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 2
        assert all(item["severity"] == "high" for item in data["items"])

    def test_list_alerts_filter_by_status(self, client, session_factory):
        """Test filtering alerts by status."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-ALERT-STATUS",
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
            )
            session.add(anomaly)
            session.commit()

            for status in AlertStatus:
                alert = Alert(
                    anomaly_id=anomaly.id,
                    inventory_id=inventory.id,
                    severity=AlertSeverity.HIGH,
                    status=status,
                    title=f"Alert {status.value}",
                )
                session.add(alert)
            session.commit()

        # Filter by TRIGGERED
        response = client.get("/api/v1/alerts?status=triggered")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["status"] == "triggered"


class TestGetAlertEndpoint:
    """Test GET /api/v1/alerts/{alert_id} endpoint."""

    def test_get_alert_success(self, client, session_factory):
        """Test getting a specific alert."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-GET-ALERT",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            anomaly = Anomaly(
                inventory_id=inventory.id,
                anomaly_type="test_anomaly",
                severity=AnomalySeverity.CRITICAL,
                score=95.0,
                status=AnomalyStatus.OPEN,
            )
            session.add(anomaly)
            session.commit()

            alert = Alert(
                anomaly_id=anomaly.id,
                inventory_id=inventory.id,
                severity=AlertSeverity.CRITICAL,
                status=AlertStatus.TRIGGERED,
                title="Test Alert",
            )
            session.add(alert)
            session.commit()
            alert_id = alert.id

        response = client.get(f"/api/v1/alerts/{alert_id}")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["id"] == str(alert_id)
        assert data["title"] == "Test Alert"
        assert data["status"] == "triggered"

    def test_get_alert_not_found(self, client):
        """Test getting a non-existent alert."""
        alert_id = uuid4()
        response = client.get(f"/api/v1/alerts/{alert_id}")

        assert response.status_code == status.HTTP_404_NOT_FOUND


class TestAcknowledgeAlertEndpoint:
    """Test POST /api/v1/alerts/{alert_id}/acknowledge endpoint."""

    def test_acknowledge_alert_success(self, client, session_factory):
        """Test acknowledging an alert."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-ACK-ALERT-API",
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
            )
            session.add(anomaly)
            session.commit()

            alert = Alert(
                anomaly_id=anomaly.id,
                inventory_id=inventory.id,
                severity=AlertSeverity.HIGH,
                status=AlertStatus.TRIGGERED,
                title="Test Alert",
            )
            session.add(alert)
            session.commit()
            alert_id = alert.id

        response = client.post(
            f"/api/v1/alerts/{alert_id}/acknowledge",
            json={"acknowledged_by": "test_user"},
        )

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "acknowledged"
        assert data["acknowledged_by"] == "test_user"
        assert data["acknowledged_at"] is not None

    def test_acknowledge_alert_not_found(self, client):
        """Test acknowledging a non-existent alert."""
        alert_id = uuid4()
        response = client.post(
            f"/api/v1/alerts/{alert_id}/acknowledge",
            json={"acknowledged_by": "test_user"},
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND


class TestResolveAlertEndpoint:
    """Test POST /api/v1/alerts/{alert_id}/resolve endpoint."""

    def test_resolve_alert_success(self, client, session_factory):
        """Test resolving an alert."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-RESOLVE-ALERT-API",
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
            )
            session.add(anomaly)
            session.commit()

            alert = Alert(
                anomaly_id=anomaly.id,
                inventory_id=inventory.id,
                severity=AlertSeverity.HIGH,
                status=AlertStatus.ACKNOWLEDGED,
                title="Test Alert",
                acknowledged_by="test_user",
                acknowledged_at=datetime.now(UTC),
            )
            session.add(alert)
            session.commit()
            alert_id = alert.id

        response = client.post(
            f"/api/v1/alerts/{alert_id}/resolve",
            json={"resolved_by": "test_user"},
        )

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "resolved"
        assert data["resolved_at"] is not None

    def test_resolve_alert_not_found(self, client):
        """Test resolving a non-existent alert."""
        alert_id = uuid4()
        response = client.post(
            f"/api/v1/alerts/{alert_id}/resolve",
            json={"resolved_by": "test_user"},
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND


class TestSuppressAlertEndpoint:
    """Test POST /api/v1/alerts/{alert_id}/suppress endpoint."""

    def test_suppress_alert_success(self, client, session_factory):
        """Test suppressing an alert."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-SUPPRESS-ALERT-API",
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
            )
            session.add(anomaly)
            session.commit()

            alert = Alert(
                anomaly_id=anomaly.id,
                inventory_id=inventory.id,
                severity=AlertSeverity.HIGH,
                status=AlertStatus.TRIGGERED,
                title="Test Alert",
            )
            session.add(alert)
            session.commit()
            alert_id = alert.id

        suppressed_until = (datetime.now(UTC) + timedelta(hours=1)).isoformat()

        response = client.post(
            f"/api/v1/alerts/{alert_id}/suppress",
            json={"suppressed_until": suppressed_until},
        )

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "suppressed"
        assert data["suppressed_until"] is not None

    def test_suppress_alert_not_found(self, client):
        """Test suppressing a non-existent alert."""
        alert_id = uuid4()
        suppressed_until = (datetime.now(UTC) + timedelta(hours=1)).isoformat()

        response = client.post(
            f"/api/v1/alerts/{alert_id}/suppress",
            json={"suppressed_until": suppressed_until},
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND


class TestAlertStatsEndpoint:
    """Test GET /api/v1/alerts/stats/summary endpoint."""

    def test_get_alert_stats(self, client, session_factory):
        """Test getting alert statistics."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-STATS-ALERT-API",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            anomaly = Anomaly(
                inventory_id=inventory.id,
                anomaly_type="test_anomaly",
                severity=AnomalySeverity.CRITICAL,
                score=95.0,
                status=AnomalyStatus.OPEN,
            )
            session.add(anomaly)
            session.commit()

            # Create alerts with different statuses
            statuses = [
                (AlertStatus.TRIGGERED, 2),
                (AlertStatus.ACKNOWLEDGED, 1),
                (AlertStatus.RESOLVED, 1),
            ]

            for status, count in statuses:
                for i in range(count):
                    alert = Alert(
                        anomaly_id=anomaly.id,
                        inventory_id=inventory.id,
                        severity=AlertSeverity.CRITICAL,
                        status=status,
                        title=f"Alert {status.value} {i}",
                    )
                    session.add(alert)
            session.commit()

        response = client.get("/api/v1/alerts/stats/summary")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total_alerts"] == 4
        assert data["triggered_count"] == 2
        assert data["acknowledged_count"] == 1
        assert data["resolved_count"] == 1
        assert data["critical_count"] == 4
        assert data["high_count"] == 0

    def test_get_alert_stats_empty(self, client):
        """Test getting alert statistics when no alerts exist."""
        response = client.get("/api/v1/alerts/stats/summary")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total_alerts"] == 0
        assert data["triggered_count"] == 0
