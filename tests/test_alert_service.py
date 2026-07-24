"""Unit tests for alert service (queries and mutations)."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
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
from app.services.alert_service import (
    AlertNotFoundError,
    AlertService,
    AlertTransientError,
)


class TestAlertServiceQueries:
    """Test alert service query operations."""

    def test_list_alerts_empty(self, session_factory):
        """Test listing alerts when none exist."""
        service = AlertService(session_factory=session_factory)
        result = service.list_alerts(skip=0, limit=20)

        assert result.total == 0
        assert len(result.alerts) == 0

    def test_list_alerts_with_results(self, session_factory):
        """Test listing alerts with results."""
        # Create test data
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-ALERT-LIST",
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
                description="Test alert description",
            )
            session.add(alert)
            session.commit()

        service = AlertService(session_factory=session_factory)
        result = service.list_alerts(skip=0, limit=20)

        assert result.total == 1
        assert len(result.alerts) == 1
        assert result.alerts[0].title == "Test Alert"

    def test_list_alerts_pagination(self, session_factory):
        """Test alert listing with pagination."""
        # Create multiple alerts
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-ALERT-PAGINATE",
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
                    score=50.0 + i,
                    status=AnomalyStatus.OPEN,
                )
                session.add(anomaly)
            session.commit()

            for i, anomaly_id in enumerate(session.query(Anomaly.id).all()):
                alert = Alert(
                    anomaly_id=anomaly_id[0],
                    inventory_id=inventory.id,
                    severity=AlertSeverity.HIGH,
                    status=AlertStatus.TRIGGERED,
                    title=f"Alert {i}",
                )
                session.add(alert)
            session.commit()

        service = AlertService(session_factory=session_factory)

        # Get first page
        result1 = service.list_alerts(skip=0, limit=10)
        assert result1.total == 25
        assert len(result1.alerts) == 10

        # Get second page
        result2 = service.list_alerts(skip=10, limit=10)
        assert result2.total == 25
        assert len(result2.alerts) == 10

        # Verify no duplicates
        ids1 = {a.id for a in result1.alerts}
        ids2 = {a.id for a in result2.alerts}
        assert len(ids1 & ids2) == 0  # No overlap

    def test_list_alerts_filter_by_severity(self, session_factory):
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
                alert = Alert(
                    anomaly_id=anomaly.id,
                    inventory_id=inventory.id,
                    severity=(
                        AlertSeverity.CRITICAL
                        if anomaly.severity == AnomalySeverity.CRITICAL
                        else AlertSeverity.HIGH
                    ),
                    status=AlertStatus.TRIGGERED,
                    title="Test Alert",
                )
                session.add(alert)
            session.commit()

        service = AlertService(session_factory=session_factory)

        # Filter by CRITICAL
        result = service.list_alerts(severity=AlertSeverity.CRITICAL)
        assert result.total == 3
        assert all(a.severity == AlertSeverity.CRITICAL for a in result.alerts)

        # Filter by HIGH
        result = service.list_alerts(severity=AlertSeverity.HIGH)
        assert result.total == 2
        assert all(a.severity == AlertSeverity.HIGH for a in result.alerts)

    def test_list_alerts_filter_by_status(self, session_factory):
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

        service = AlertService(session_factory=session_factory)

        # Filter by TRIGGERED
        result = service.list_alerts(status=AlertStatus.TRIGGERED)
        assert result.total == 1
        assert result.alerts[0].status == AlertStatus.TRIGGERED

    def test_get_alert(self, session_factory):
        """Test getting a specific alert."""
        # Create test data
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-ALERT-GET",
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

        service = AlertService(session_factory=session_factory)
        retrieved_alert = service.get_alert(str(alert_id))

        assert retrieved_alert.id == alert_id
        assert retrieved_alert.title == "Test Alert"

    def test_get_alert_not_found(self, session_factory):
        """Test getting a non-existent alert."""
        service = AlertService(session_factory=session_factory)

        with pytest.raises(AlertNotFoundError):
            service.get_alert(str(uuid4()))

    def test_get_alert_by_anomaly(self, session_factory):
        """Test getting the most recent alert for an anomaly."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-ALERT-BY-ANOMALY",
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

            # Create multiple alerts for same anomaly
            for i in range(3):
                alert = Alert(
                    anomaly_id=anomaly.id,
                    inventory_id=inventory.id,
                    severity=AlertSeverity.HIGH,
                    status=AlertStatus.TRIGGERED if i < 2 else AlertStatus.RESOLVED,
                    title=f"Alert {i}",
                )
                session.add(alert)
            session.commit()

        service = AlertService(session_factory=session_factory)
        alert = service.get_alert_by_anomaly(str(anomaly.id))

        assert alert is not None
        assert alert.status != AlertStatus.RESOLVED  # Should get non-resolved


class TestAlertServiceCreation:
    """Test alert service creation with deduplication."""

    def test_create_alert(self, session_factory):
        """Test creating an alert."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-CREATE-ALERT",
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
            anomaly_id = anomaly.id
            inventory_id = inventory.id

        service = AlertService(session_factory=session_factory)
        alert = service.create_alert(
            anomaly_id=anomaly_id,
            inventory_id=inventory_id,
            severity=AlertSeverity.CRITICAL,
            title="Critical Alert",
            description="Critical anomaly detected",
        )

        assert alert.id is not None
        assert alert.anomaly_id == anomaly_id
        assert alert.severity == AlertSeverity.CRITICAL
        assert alert.status == AlertStatus.TRIGGERED
        assert alert.title == "Critical Alert"

    def test_create_alert_deduplication(self, session_factory):
        """Test alert deduplication within time window."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-DEDUP-ALERT",
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
            anomaly_id = anomaly.id
            inventory_id = inventory.id

        service = AlertService(session_factory=session_factory)

        # Create first alert
        alert1 = service.create_alert(
            anomaly_id=anomaly_id,
            inventory_id=inventory_id,
            severity=AlertSeverity.HIGH,
            title="Alert 1",
        )

        # Create second alert for same anomaly (should be deduplicated)
        alert2 = service.create_alert(
            anomaly_id=anomaly_id,
            inventory_id=inventory_id,
            severity=AlertSeverity.HIGH,
            title="Alert 2",
        )

        # Should get same alert (deduplicated)
        assert alert1.id == alert2.id
        assert alert1.title == alert2.title


class TestAlertServiceLifecycle:
    """Test alert service lifecycle mutations."""

    def test_acknowledge_alert(self, session_factory):
        """Test acknowledging an alert."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-ACK-ALERT",
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

        service = AlertService(session_factory=session_factory)
        updated_alert = service.acknowledge_alert(str(alert_id), "admin")

        assert updated_alert.status == AlertStatus.ACKNOWLEDGED
        assert updated_alert.acknowledged_by == "admin"
        assert updated_alert.acknowledged_at is not None

    def test_resolve_alert(self, session_factory):
        """Test resolving an alert."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-RESOLVE-ALERT",
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
                acknowledged_by="admin",
                acknowledged_at=datetime.now(UTC),
            )
            session.add(alert)
            session.commit()
            alert_id = alert.id

        service = AlertService(session_factory=session_factory)
        updated_alert = service.resolve_alert(str(alert_id))

        assert updated_alert.status == AlertStatus.RESOLVED
        assert updated_alert.resolved_at is not None

    def test_suppress_alert(self, session_factory):
        """Test suppressing an alert."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-SUPPRESS-ALERT",
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

        suppressed_until = datetime.now(UTC) + timedelta(hours=1)

        service = AlertService(session_factory=session_factory)
        updated_alert = service.suppress_alert(str(alert_id), suppressed_until)

        assert updated_alert.status == AlertStatus.SUPPRESSED
        assert updated_alert.suppressed_until is not None


class TestAlertServiceStats:
    """Test alert service statistics."""

    def test_get_alert_stats(self, session_factory):
        """Test getting alert statistics."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-STATS-ALERT",
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

        service = AlertService(session_factory=session_factory)
        stats = service.get_alert_stats()

        assert stats.total == 4
        assert stats.triggered == 2
        assert stats.acknowledged == 1
        assert stats.resolved == 1
        assert stats.critical == 4
        assert stats.high == 0
