"""Unit tests for anomaly service (queries and mutations)."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from app.database.models import (
    Anomaly,
    AnomalySeverity,
    AnomalyStatus,
    Inventory,
)
from app.services.anomaly_service import (
    AnomalyNotFoundError,
    AnomalyService,
    AnomalyTransientError,
)


class TestAnomalyServiceQueries:
    """Test anomaly service query operations."""

    def test_list_anomalies_empty(self, session_factory):
        """Test listing anomalies when none exist."""
        service = AnomalyService(session_factory=session_factory)
        result = service.list_anomalies(skip=0, limit=20)

        assert result.total == 0
        assert len(result.anomalies) == 0

    def test_list_anomalies_with_results(self, session_factory):
        """Test listing anomalies with results."""
        # Create test data
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-LIST",
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

        service = AnomalyService(session_factory=session_factory)
        result = service.list_anomalies(skip=0, limit=20)

        assert result.total == 1
        assert len(result.anomalies) == 1
        assert result.anomalies[0].anomaly_type == "test_anomaly"

    def test_list_anomalies_pagination(self, session_factory):
        """Test anomaly listing with pagination."""
        # Create multiple anomalies
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-PAGINATE",
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
                    score=50.0 + i,
                    status=AnomalyStatus.OPEN,
                )
                session.add(anomaly)
            session.commit()

        service = AnomalyService(session_factory=session_factory)

        # Get first page
        result1 = service.list_anomalies(skip=0, limit=10)
        assert result1.total == 25
        assert len(result1.anomalies) == 10

        # Get second page
        result2 = service.list_anomalies(skip=10, limit=10)
        assert result2.total == 25
        assert len(result2.anomalies) == 10

        # Verify no duplicates
        ids1 = {a.id for a in result1.anomalies}
        ids2 = {a.id for a in result2.anomalies}
        assert len(ids1 & ids2) == 0

    def test_list_anomalies_filter_by_status(self, session_factory):
        """Test filtering anomalies by status."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-FILTER",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            # Add anomalies with different statuses
            for status in [AnomalyStatus.OPEN, AnomalyStatus.INVESTIGATING, AnomalyStatus.RESOLVED]:
                anomaly = Anomaly(
                    inventory_id=inventory.id,
                    anomaly_type="test",
                    severity=AnomalySeverity.MEDIUM,
                    score=60.0,
                    status=status,
                )
                session.add(anomaly)
            session.commit()

        service = AnomalyService(session_factory=session_factory)

        # Filter by OPEN status
        result = service.list_anomalies(status=AnomalyStatus.OPEN)
        assert result.total == 1
        assert result.anomalies[0].status == AnomalyStatus.OPEN

    def test_list_anomalies_filter_by_severity(self, session_factory):
        """Test filtering anomalies by severity."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-SEV",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add(inventory)
            session.commit()

            # Add anomalies with different severities
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

        service = AnomalyService(session_factory=session_factory)

        # Filter by CRITICAL severity
        result = service.list_anomalies(severity=AnomalySeverity.CRITICAL)
        assert result.total == 1
        assert result.anomalies[0].severity == AnomalySeverity.CRITICAL

    def test_get_anomaly_success(self, session_factory):
        """Test retrieving a specific anomaly."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-GET",
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
                description="Test anomaly for retrieval",
            )
            session.add(anomaly)
            session.commit()
            anomaly_id = anomaly.id

        service = AnomalyService(session_factory=session_factory)
        retrieved = service.get_anomaly(str(anomaly_id))

        assert retrieved.id == anomaly_id
        assert retrieved.anomaly_type == "test"
        assert retrieved.description == "Test anomaly for retrieval"

    def test_get_anomaly_not_found(self, session_factory):
        """Test retrieving non-existent anomaly."""
        service = AnomalyService(session_factory=session_factory)

        with pytest.raises(AnomalyNotFoundError):
            service.get_anomaly(str(uuid4()))

    def test_get_anomalies_by_inventory(self, session_factory):
        """Test retrieving anomalies by inventory."""
        with session_factory() as session:
            inventory1 = Inventory(
                sku="SKU-INV1",
                warehouse_id="WH-1",
                quantity=100,
            )
            inventory2 = Inventory(
                sku="SKU-INV2",
                warehouse_id="WH-1",
                quantity=100,
            )
            session.add_all([inventory1, inventory2])
            session.commit()

            # Add anomalies to both inventories
            for i in range(3):
                session.add(Anomaly(
                    inventory_id=inventory1.id,
                    anomaly_type=f"test_{i}",
                    severity=AnomalySeverity.LOW,
                    score=50.0,
                    status=AnomalyStatus.OPEN,
                ))
            for i in range(2):
                session.add(Anomaly(
                    inventory_id=inventory2.id,
                    anomaly_type=f"test_{i}",
                    severity=AnomalySeverity.LOW,
                    score=50.0,
                    status=AnomalyStatus.OPEN,
                ))
            session.commit()

        service = AnomalyService(session_factory=session_factory)

        # Get anomalies for inventory1
        anomalies = service.get_anomalies_by_inventory(str(inventory1.id))
        assert len(anomalies) == 3
        assert all(a.inventory_id == inventory1.id for a in anomalies)

    def test_get_stats(self, session_factory):
        """Test getting anomaly statistics."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-STATS",
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

        service = AnomalyService(session_factory=session_factory)
        stats = service.get_stats()

        assert stats["total_anomalies"] == 4
        assert stats["open_anomalies"] == 2
        assert stats["investigating_anomalies"] == 1
        assert stats["resolved_anomalies"] == 1
        assert stats["critical_count"] == 1
        assert stats["high_count"] == 1
        assert stats["medium_count"] == 1
        assert stats["low_count"] == 1
        assert len(stats["anomaly_types_count"]) == 4


class TestAnomalyServiceMutations:
    """Test anomaly service mutation operations."""

    def test_update_anomaly_status_to_investigating(self, session_factory):
        """Test updating anomaly status to investigating."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-UPDATE",
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

        service = AnomalyService(session_factory=session_factory)
        updated = service.update_anomaly_status(
            str(anomaly_id),
            AnomalyStatus.INVESTIGATING,
        )

        assert updated.status == AnomalyStatus.INVESTIGATING
        assert updated.resolved_at is None

    def test_update_anomaly_status_to_resolved(self, session_factory):
        """Test updating anomaly status to resolved."""
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-RESOLVE",
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

        service = AnomalyService(session_factory=session_factory)
        updated = service.update_anomaly_status(
            str(anomaly_id),
            AnomalyStatus.RESOLVED,
        )

        assert updated.status == AnomalyStatus.RESOLVED
        assert updated.resolved_at is not None

    def test_update_nonexistent_anomaly(self, session_factory):
        """Test updating non-existent anomaly."""
        service = AnomalyService(session_factory=session_factory)

        with pytest.raises(AnomalyNotFoundError):
            service.update_anomaly_status(
                str(uuid4()),
                AnomalyStatus.RESOLVED,
            )
