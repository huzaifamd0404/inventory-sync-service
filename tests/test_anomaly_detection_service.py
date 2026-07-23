"""Unit tests for anomaly detection service."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from app.database.models import (
    Anomaly,
    AnomalySeverity,
    AnomalyStatus,
    Inventory,
    InventoryChangeType,
    InventoryHistory,
    Sales,
)
from app.services.anomaly_detection_service import (
    AnomalyDetectionResult,
    AnomalyDetectionService,
    LargeInventoryAdjustment,
    NegativeInventoryRule,
    RapidConsecutiveSales,
    SuddenSalesSpike,
)


class TestNegativeInventoryRule:
    """Test negative inventory detection rule."""

    def test_detect_negative_inventory(self, session: Session):
        """Test detection of negative inventory."""
        inventory = Inventory(
            sku="SKU-NEG",
            warehouse_id="WH-1",
            quantity=-5,
        )
        session.add(inventory)
        session.commit()

        rule = NegativeInventoryRule()
        result = rule.evaluate(inventory, session)

        assert result.detected is True
        assert result.anomaly_type == "negative_inventory"
        assert result.severity == AnomalySeverity.CRITICAL
        assert result.score == 100.0

    def test_positive_inventory_no_anomaly(self, session: Session):
        """Test that positive inventory doesn't trigger anomaly."""
        inventory = Inventory(
            sku="SKU-POS",
            warehouse_id="WH-1",
            quantity=10,
        )
        session.add(inventory)
        session.commit()

        rule = NegativeInventoryRule()
        result = rule.evaluate(inventory, session)

        assert result.detected is False


class TestSuddenSalesSpike:
    """Test sudden sales spike detection rule."""

    def test_detect_sales_spike(self, session: Session):
        """Test detection of sudden sales spike."""
        inventory = Inventory(
            sku="SKU-SPIKE",
            warehouse_id="WH-1",
            quantity=100,
        )
        session.add(inventory)
        session.commit()

        # Add historical sales (low volume)
        now = datetime.now(UTC)
        for i in range(10):
            sale = Sales(
                inventory_id=inventory.id,
                quantity_sold=2,
                sold_at=now - timedelta(hours=24-i),
            )
            session.add(sale)

        # Add spike sale
        spike_sale = Sales(
            inventory_id=inventory.id,
            quantity_sold=20,
            sold_at=now,
        )
        session.add(spike_sale)
        session.commit()

        rule = SuddenSalesSpike(spike_multiplier=5.0, lookback_hours=24, min_prior_sales=5)
        result = rule.evaluate(inventory, session)

        assert result.detected is True
        assert result.anomaly_type == "sudden_sales_spike"
        assert result.severity in [AnomalySeverity.HIGH, AnomalySeverity.CRITICAL]
        assert result.score > 0

    def test_no_spike_with_insufficient_history(self, session: Session):
        """Test no anomaly with insufficient sales history."""
        inventory = Inventory(
            sku="SKU-NOSPIKE",
            warehouse_id="WH-1",
            quantity=100,
        )
        session.add(inventory)
        session.commit()

        # Add only 2 sales
        now = datetime.now(UTC)
        for i in range(2):
            sale = Sales(
                inventory_id=inventory.id,
                quantity_sold=10,
                sold_at=now - timedelta(hours=i),
            )
            session.add(sale)
        session.commit()

        rule = SuddenSalesSpike(spike_multiplier=5.0, lookback_hours=24, min_prior_sales=5)
        result = rule.evaluate(inventory, session)

        assert result.detected is False


class TestLargeInventoryAdjustment:
    """Test large inventory adjustment detection rule."""

    def test_detect_large_adjustment(self, session: Session):
        """Test detection of large inventory adjustment."""
        inventory = Inventory(
            sku="SKU-ADJ",
            warehouse_id="WH-1",
            quantity=100,
        )
        session.add(inventory)
        session.commit()

        # Add history entry showing large adjustment
        history = InventoryHistory(
            inventory_id=inventory.id,
            change_type=InventoryChangeType.ADJUSTMENT,
            quantity_before=100,
            quantity_after=30,
            quantity_delta=-70,
        )
        session.add(history)
        session.commit()

        rule = LargeInventoryAdjustment(adjustment_threshold_percent=50.0, lookback_hours=24)
        result = rule.evaluate(inventory, session)

        assert result.detected is True
        assert result.anomaly_type == "large_inventory_adjustment"
        assert result.severity in [AnomalySeverity.MEDIUM, AnomalySeverity.HIGH]

    def test_small_adjustment_no_anomaly(self, session: Session):
        """Test that small adjustment doesn't trigger anomaly."""
        inventory = Inventory(
            sku="SKU-SMALL-ADJ",
            warehouse_id="WH-1",
            quantity=100,
        )
        session.add(inventory)
        session.commit()

        # Add small adjustment
        history = InventoryHistory(
            inventory_id=inventory.id,
            change_type=InventoryChangeType.ADJUSTMENT,
            quantity_before=100,
            quantity_after=95,
            quantity_delta=-5,
        )
        session.add(history)
        session.commit()

        rule = LargeInventoryAdjustment(adjustment_threshold_percent=50.0, lookback_hours=24)
        result = rule.evaluate(inventory, session)

        assert result.detected is False


class TestRapidConsecutiveSales:
    """Test rapid consecutive sales detection rule."""

    def test_detect_rapid_sales(self, session: Session):
        """Test detection of rapid consecutive sales."""
        inventory = Inventory(
            sku="SKU-RAPID",
            warehouse_id="WH-1",
            quantity=100,
        )
        session.add(inventory)
        session.commit()

        # Add rapid consecutive sales
        now = datetime.now(UTC)
        for i in range(7):
            sale = Sales(
                inventory_id=inventory.id,
                quantity_sold=1,
                sold_at=now - timedelta(seconds=10 + i),
            )
            session.add(sale)
        session.commit()

        rule = RapidConsecutiveSales(transaction_count=5, time_window_seconds=60)
        result = rule.evaluate(inventory, session)

        assert result.detected is True
        assert result.anomaly_type == "rapid_consecutive_sales"
        assert result.severity in [AnomalySeverity.MEDIUM, AnomalySeverity.HIGH]

    def test_no_rapid_sales_when_spaced_out(self, session: Session):
        """Test no anomaly when sales are spaced out."""
        inventory = Inventory(
            sku="SKU-SPACED",
            warehouse_id="WH-1",
            quantity=100,
        )
        session.add(inventory)
        session.commit()

        # Add spaced out sales
        now = datetime.now(UTC)
        for i in range(5):
            sale = Sales(
                inventory_id=inventory.id,
                quantity_sold=1,
                sold_at=now - timedelta(seconds=300 + i*10),
            )
            session.add(sale)
        session.commit()

        rule = RapidConsecutiveSales(transaction_count=5, time_window_seconds=60)
        result = rule.evaluate(inventory, session)

        assert result.detected is False


class TestAnomalyDetectionService:
    """Test anomaly detection service."""

    def test_detect_multiple_anomalies(self, session_factory):
        """Test detection of multiple anomalies."""
        service = AnomalyDetectionService(session_factory=session_factory)

        # Create inventory with negative quantity
        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-MULTI",
                warehouse_id="WH-1",
                quantity=-10,
            )
            session.add(inventory)
            session.commit()

            # Detect anomalies
            anomalies = service.detect_anomalies(str(inventory.id), event_id="EVT-001")

            assert len(anomalies) > 0
            assert any(a.anomaly_type == "negative_inventory" for a in anomalies)

    def test_persist_anomalies(self, session_factory):
        """Test persisting anomalies."""
        service = AnomalyDetectionService(session_factory=session_factory)

        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-PERSIST",
                warehouse_id="WH-1",
                quantity=-5,
            )
            session.add(inventory)
            session.commit()

            # Detect and persist
            anomalies = service.detect_anomalies(str(inventory.id), event_id="EVT-002")
            service.persist_anomalies(anomalies)

            # Verify persisted
            with session_factory() as verify_session:
                persisted = verify_session.query(Anomaly).filter_by(
                    inventory_id=inventory.id
                ).all()

                assert len(persisted) > 0
                assert persisted[0].event_id == "EVT-002"
                assert persisted[0].status == AnomalyStatus.OPEN

    def test_idempotent_persistence(self, session_factory):
        """Test that duplicate anomalies are not persisted."""
        service = AnomalyDetectionService(session_factory=session_factory)

        with session_factory() as session:
            inventory = Inventory(
                sku="SKU-IDEM",
                warehouse_id="WH-1",
                quantity=-5,
            )
            session.add(inventory)
            session.commit()

            # Detect and persist
            anomalies = service.detect_anomalies(str(inventory.id), event_id="EVT-003")
            service.persist_anomalies(anomalies)

            # Try to persist same event again
            anomalies2 = service.detect_anomalies(str(inventory.id), event_id="EVT-003")
            service.persist_anomalies(anomalies2)

            # Verify only one was persisted
            with session_factory() as verify_session:
                persisted = verify_session.query(Anomaly).filter_by(
                    event_id="EVT-003"
                ).all()

                assert len(persisted) == 1

    def test_add_custom_rule(self, session_factory):
        """Test adding custom rule to service."""

        class CustomRule:
            def evaluate(self, inventory, session):
                return AnomalyDetectionResult(detected=False)

        service = AnomalyDetectionService(session_factory=session_factory)
        initial_count = len(service._rules)

        custom_rule = CustomRule()
        service.add_rule(custom_rule)

        assert len(service._rules) == initial_count + 1

    def test_remove_rule(self, session_factory):
        """Test removing rule from service."""
        service = AnomalyDetectionService(session_factory=session_factory)
        initial_count = len(service._rules)

        removed = service.remove_rule(NegativeInventoryRule)

        assert removed is True
        assert len(service._rules) == initial_count - 1

    def test_remove_nonexistent_rule(self, session_factory):
        """Test removing rule that doesn't exist."""

        class NonexistentRule:
            pass

        service = AnomalyDetectionService(session_factory=session_factory)
        removed = service.remove_rule(NonexistentRule)

        assert removed is False
