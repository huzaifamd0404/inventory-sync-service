"""Rule-based anomaly detection engine for inventory and sales events."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.database.models import (
    Anomaly,
    AnomalySeverity,
    AnomalyStatus,
    Inventory,
    InventoryHistory,
    Sales,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AnomalyDetectionResult:
    """Result from anomaly detection analysis."""

    detected: bool
    anomaly_type: str | None = None
    severity: AnomalySeverity | None = None
    score: float = 0.0
    description: str | None = None


class AnomalyDetectionRule(ABC):
    """Base class for anomaly detection rules."""

    @abstractmethod
    def evaluate(
        self,
        inventory: Inventory,
        session: Session,
    ) -> AnomalyDetectionResult:
        """Evaluate the rule against inventory state.

        Args:
            inventory: The inventory item to evaluate
            session: Database session for querying related data

        Returns:
            AnomalyDetectionResult indicating if anomaly was detected
        """


class NegativeInventoryRule(AnomalyDetectionRule):
    """Detect negative inventory levels."""

    def evaluate(self, inventory: Inventory, session: Session) -> AnomalyDetectionResult:
        if inventory.quantity < 0:
            return AnomalyDetectionResult(
                detected=True,
                anomaly_type="negative_inventory",
                severity=AnomalySeverity.CRITICAL,
                score=100.0,
                description=f"Negative inventory detected: {inventory.quantity} units for SKU {inventory.sku}",
            )
        return AnomalyDetectionResult(detected=False)


class SuddenSalesSpike(AnomalyDetectionRule):
    """Detect sudden spikes in sales quantity."""

    def __init__(
        self,
        spike_multiplier: float = 5.0,
        lookback_hours: int = 24,
        min_prior_sales: int = 5,
    ):
        """Initialize spike detection rule.

        Args:
            spike_multiplier: How many times the avg sale qty to trigger spike
            lookback_hours: Hours to look back for historical sales
            min_prior_sales: Minimum prior sales to establish baseline
        """
        self.spike_multiplier = spike_multiplier
        self.lookback_hours = lookback_hours
        self.min_prior_sales = min_prior_sales

    def evaluate(self, inventory: Inventory, session: Session) -> AnomalyDetectionResult:
        # Get recent sales history
        cutoff = datetime.now(UTC) - timedelta(hours=self.lookback_hours)
        sales_history = session.execute(
            select(Sales)
            .where(
                and_(
                    Sales.inventory_id == inventory.id,
                    Sales.sold_at >= cutoff,
                )
            )
            .order_by(Sales.sold_at.desc())
        ).scalars().all()

        if len(sales_history) < self.min_prior_sales:
            return AnomalyDetectionResult(detected=False)

        # Calculate average sale quantity
        quantities = [s.quantity_sold for s in sales_history]
        avg_quantity = sum(quantities) / len(quantities)
        max_quantity = max(quantities)

        # Check for spike
        if max_quantity > avg_quantity * self.spike_multiplier:
            deviation_ratio = max_quantity / avg_quantity
            severity = self._calculate_severity(deviation_ratio)
            score = min(100.0, (deviation_ratio / self.spike_multiplier) * 100)

            return AnomalyDetectionResult(
                detected=True,
                anomaly_type="sudden_sales_spike",
                severity=severity,
                score=score,
                description=(
                    f"Sales spike detected: maximum sale {max_quantity} units is "
                    f"{deviation_ratio:.1f}x the average of {avg_quantity:.1f} units"
                ),
            )

        return AnomalyDetectionResult(detected=False)

    def _calculate_severity(self, deviation_ratio: float) -> AnomalySeverity:
        """Calculate severity based on deviation ratio."""
        if deviation_ratio > 10:
            return AnomalySeverity.CRITICAL
        elif deviation_ratio > 7:
            return AnomalySeverity.HIGH
        elif deviation_ratio > 5:
            return AnomalySeverity.MEDIUM
        return AnomalySeverity.LOW


class LargeInventoryAdjustment(AnomalyDetectionRule):
    """Detect unusually large inventory adjustments."""

    def __init__(
        self,
        adjustment_threshold_percent: float = 50.0,
        lookback_hours: int = 24,
    ):
        """Initialize large adjustment detection rule.

        Args:
            adjustment_threshold_percent: Threshold for % of stock to trigger
            lookback_hours: Hours to look back for average daily volume
        """
        self.adjustment_threshold_percent = adjustment_threshold_percent
        self.lookback_hours = lookback_hours

    def evaluate(self, inventory: Inventory, session: Session) -> AnomalyDetectionResult:
        # Get recent adjustments from history
        cutoff = datetime.now(UTC) - timedelta(hours=self.lookback_hours)
        recent_changes = session.execute(
            select(InventoryHistory)
            .where(
                and_(
                    InventoryHistory.inventory_id == inventory.id,
                    InventoryHistory.changed_at >= cutoff,
                )
            )
            .order_by(InventoryHistory.changed_at.desc())
        ).scalars().all()

        if not recent_changes:
            return AnomalyDetectionResult(detected=False)

        # Get the most recent change and evaluate
        latest_change = recent_changes[0]
        adjustment_amount = abs(latest_change.quantity_delta)
        current_quantity = inventory.quantity

        # Use maximum of before/after quantity for threshold calculation
        reference_quantity = max(abs(latest_change.quantity_before), abs(current_quantity), 1)
        adjustment_percent = (adjustment_amount / reference_quantity) * 100

        if adjustment_percent > self.adjustment_threshold_percent:
            severity = self._calculate_severity(adjustment_percent)
            score = min(100.0, adjustment_percent)

            return AnomalyDetectionResult(
                detected=True,
                anomaly_type="large_inventory_adjustment",
                severity=severity,
                score=score,
                description=(
                    f"Large adjustment detected: {adjustment_amount} units ({adjustment_percent:.1f}%) "
                    f"from {latest_change.quantity_before} to {current_quantity} units"
                ),
            )

        return AnomalyDetectionResult(detected=False)

    def _calculate_severity(self, adjustment_percent: float) -> AnomalySeverity:
        """Calculate severity based on adjustment percentage."""
        if adjustment_percent > 150:
            return AnomalySeverity.CRITICAL
        elif adjustment_percent > 100:
            return AnomalySeverity.HIGH
        elif adjustment_percent > 75:
            return AnomalySeverity.MEDIUM
        return AnomalySeverity.LOW


class RapidConsecutiveSales(AnomalyDetectionRule):
    """Detect rapid consecutive sales transactions."""

    def __init__(
        self,
        transaction_count: int = 5,
        time_window_seconds: int = 60,
    ):
        """Initialize rapid sales detection rule.

        Args:
            transaction_count: Number of sales to trigger rule
            time_window_seconds: Time window for consecutive sales
        """
        self.transaction_count = transaction_count
        self.time_window_seconds = time_window_seconds

    def evaluate(self, inventory: Inventory, session: Session) -> AnomalyDetectionResult:
        # Get recent sales
        cutoff = datetime.now(UTC) - timedelta(seconds=self.time_window_seconds * 2)
        recent_sales = session.execute(
            select(Sales)
            .where(
                and_(
                    Sales.inventory_id == inventory.id,
                    Sales.sold_at >= cutoff,
                )
            )
            .order_by(Sales.sold_at.desc())
        ).scalars().all()

        if len(recent_sales) < self.transaction_count:
            return AnomalyDetectionResult(detected=False)

        # Check if last N transactions are within time window
        recent_window = datetime.now(UTC) - timedelta(seconds=self.time_window_seconds)
        rapid_sales = [s for s in recent_sales if s.sold_at >= recent_window]

        if len(rapid_sales) >= self.transaction_count:
            total_quantity = sum(s.quantity_sold for s in rapid_sales)
            severity = self._calculate_severity(len(rapid_sales))
            score = min(100.0, (len(rapid_sales) / self.transaction_count) * 100)

            return AnomalyDetectionResult(
                detected=True,
                anomaly_type="rapid_consecutive_sales",
                severity=severity,
                score=score,
                description=(
                    f"Rapid sales detected: {len(rapid_sales)} transactions ({total_quantity} units total) "
                    f"in {self.time_window_seconds} seconds"
                ),
            )

        return AnomalyDetectionResult(detected=False)

    def _calculate_severity(self, transaction_count: int) -> AnomalySeverity:
        """Calculate severity based on transaction count."""
        if transaction_count > 15:
            return AnomalySeverity.CRITICAL
        elif transaction_count > 10:
            return AnomalySeverity.HIGH
        elif transaction_count > 7:
            return AnomalySeverity.MEDIUM
        return AnomalySeverity.LOW


class AnomalyDetectionServiceError(Exception):
    """Base exception for anomaly detection service."""


class AnomalyDetectionService:
    """Production-ready rule-based anomaly detection engine."""

    def __init__(
        self,
        session_factory,
        rules: list[AnomalyDetectionRule] | None = None,
    ):
        """Initialize anomaly detection service.

        Args:
            session_factory: Callable that returns a database session
            rules: List of anomaly detection rules; if None, uses default rules
        """
        self._session_factory = session_factory
        self._rules = rules or self._create_default_rules()

    def _create_default_rules(self) -> list[AnomalyDetectionRule]:
        """Create default set of anomaly detection rules."""
        return [
            NegativeInventoryRule(),
            SuddenSalesSpike(spike_multiplier=5.0, lookback_hours=24, min_prior_sales=5),
            LargeInventoryAdjustment(adjustment_threshold_percent=50.0, lookback_hours=24),
            RapidConsecutiveSales(transaction_count=5, time_window_seconds=60),
        ]

    def detect_anomalies(
        self,
        inventory_id: str,
        event_id: str | None = None,
    ) -> list[Anomaly]:
        """Detect anomalies for a specific inventory item.

        Args:
            inventory_id: UUID of the inventory item to analyze
            event_id: Optional source event ID that triggered detection

        Returns:
            List of detected Anomaly records

        Raises:
            AnomalyDetectionServiceError: If detection fails
        """
        try:
            with self._session_factory() as session:
                # Fetch inventory
                inventory = session.get(Inventory, inventory_id)
                if inventory is None:
                    logger.warning(
                        "inventory_not_found_for_anomaly_detection",
                        extra={"inventory_id": str(inventory_id)},
                    )
                    return []

                # Run all rules and collect results
                anomalies: list[Anomaly] = []
                for rule in self._rules:
                    try:
                        result = rule.evaluate(inventory, session)
                        if result.detected and result.anomaly_type and result.severity:
                            anomaly = Anomaly(
                                inventory_id=inventory.id,
                                event_id=event_id,
                                anomaly_type=result.anomaly_type,
                                severity=result.severity,
                                score=result.score,
                                status=AnomalyStatus.OPEN,
                                description=result.description,
                            )
                            anomalies.append(anomaly)
                            logger.info(
                                "anomaly_detected",
                                extra={
                                    "anomaly_type": result.anomaly_type,
                                    "severity": result.severity.value,
                                    "score": result.score,
                                    "inventory_id": str(inventory.id),
                                    "event_id": event_id,
                                },
                            )
                    except Exception as exc:  # noqa: BLE001
                        logger.exception(
                            "anomaly_detection_rule_failed",
                            extra={
                                "rule": rule.__class__.__name__,
                                "inventory_id": str(inventory.id),
                            },
                        )
                        continue

                return anomalies

        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "anomaly_detection_service_error",
                extra={"inventory_id": str(inventory_id)},
            )
            raise AnomalyDetectionServiceError("anomaly detection failed") from exc

    def persist_anomalies(self, anomalies: list[Anomaly]) -> None:
        """Persist detected anomalies to database.

        Args:
            anomalies: List of detected Anomaly objects to persist

        Raises:
            AnomalyDetectionServiceError: If persistence fails
        """
        if not anomalies:
            return

        try:
            with self._session_factory() as session:
                with session.begin():
                    for anomaly in anomalies:
                        # Check if anomaly already exists for this event
                        if anomaly.event_id:
                            existing = session.execute(
                                select(Anomaly).where(Anomaly.event_id == anomaly.event_id)
                            ).scalar_one_or_none()

                            if existing is not None:
                                logger.info(
                                    "anomaly_already_exists",
                                    extra={"event_id": anomaly.event_id},
                                )
                                continue

                        session.add(anomaly)

                    session.flush()
                    logger.info(
                        "anomalies_persisted",
                        extra={"count": len(anomalies)},
                    )

        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "anomaly_persistence_error",
                extra={"count": len(anomalies)},
            )
            raise AnomalyDetectionServiceError("failed to persist anomalies") from exc

    def add_rule(self, rule: AnomalyDetectionRule) -> None:
        """Add a new anomaly detection rule.

        Args:
            rule: The rule to add
        """
        self._rules.append(rule)
        logger.info(
            "anomaly_detection_rule_added",
            extra={"rule": rule.__class__.__name__},
        )

    def remove_rule(self, rule_type: type[AnomalyDetectionRule]) -> bool:
        """Remove anomaly detection rule by type.

        Args:
            rule_type: The rule class to remove

        Returns:
            True if rule was removed, False if not found
        """
        for i, rule in enumerate(self._rules):
            if isinstance(rule, rule_type):
                self._rules.pop(i)
                logger.info(
                    "anomaly_detection_rule_removed",
                    extra={"rule": rule_type.__name__},
                )
                return True
        return False


def get_anomaly_detection_service() -> AnomalyDetectionService:
    """Dependency injection factory for AnomalyDetectionService.

    Returns:
        Configured AnomalyDetectionService instance
    """
    from app.database.session import SessionLocal

    return AnomalyDetectionService(session_factory=SessionLocal)
