"""Service for managing alerts."""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.database.models import Alert, AlertSeverity, AlertStatus, Anomaly
from app.database.session import SessionLocal

logger = logging.getLogger(__name__)


class AlertServiceError(Exception):
    """Base exception for alert service failures."""


class AlertTransientError(AlertServiceError):
    """Raised for transient failures that can be retried safely."""


class AlertNotFoundError(AlertServiceError):
    """Raised when requested alert is not found."""


class DuplicateAlertError(AlertServiceError):
    """Raised when alert would be a duplicate based on deduplication rules."""


@dataclass(frozen=True)
class AlertQueryResult:
    """Result from alert query."""

    alerts: list[Alert]
    total: int


@dataclass(frozen=True)
class AlertStatsData:
    """Statistics about alerts."""

    total: int
    triggered: int
    acknowledged: int
    resolved: int
    suppressed: int
    critical: int
    high: int
    avg_ack_time: float | None
    avg_resolution_time: float | None


class AlertService:
    """Service for alert queries, mutations, and lifecycle management."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory
        self._settings = get_settings()

    # ------------------------------------------------------------------
    # Query operations
    # ------------------------------------------------------------------

    def list_alerts(
        self,
        skip: int = 0,
        limit: int = 20,
        inventory_id: str | None = None,
        anomaly_id: str | None = None,
        severity: AlertSeverity | None = None,
        status: AlertStatus | None = None,
    ) -> AlertQueryResult:
        """List alerts with optional filtering and pagination.

        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            inventory_id: Filter by inventory ID
            anomaly_id: Filter by anomaly ID
            severity: Filter by severity level
            status: Filter by status

        Returns:
            AlertQueryResult with alerts and total count

        Raises:
            AlertTransientError: If query fails
        """
        try:
            with self._session_factory() as session:
                # Build query with filters
                query = select(Alert)

                filters = []
                if inventory_id:
                    filters.append(Alert.inventory_id == inventory_id)
                if anomaly_id:
                    filters.append(Alert.anomaly_id == anomaly_id)
                if severity:
                    filters.append(Alert.severity == severity)
                if status:
                    filters.append(Alert.status == status)

                if filters:
                    query = query.where(and_(*filters))

                # Get total count
                total_query = select(func.count()).select_from(Alert)
                if filters:
                    total_query = total_query.where(and_(*filters))
                total = session.execute(total_query).scalar() or 0

                # Get paginated results sorted by triggered_at descending
                alerts = session.execute(
                    query.order_by(Alert.triggered_at.desc()).offset(skip).limit(limit)
                ).scalars().all()

                return AlertQueryResult(alerts=list(alerts), total=total)

        except OperationalError as exc:
            logger.exception("alert_list_transient_error")
            raise AlertTransientError("transient database error") from exc
        except SQLAlchemyError as exc:
            logger.exception("alert_list_database_error")
            raise AlertTransientError("database error") from exc

    def get_alert(self, alert_id: str) -> Alert:
        """Get a specific alert by ID.

        Args:
            alert_id: Alert UUID as string

        Returns:
            Alert instance

        Raises:
            AlertNotFoundError: If alert not found
            AlertTransientError: If query fails
        """
        try:
            with self._session_factory() as session:
                alert = session.execute(
                    select(Alert).where(Alert.id == alert_id)
                ).scalar_one_or_none()

                if not alert:
                    logger.warning("alert_not_found", extra={"alert_id": alert_id})
                    raise AlertNotFoundError(f"alert {alert_id} not found")

                return alert

        except AlertNotFoundError:
            raise
        except OperationalError as exc:
            logger.exception("alert_get_transient_error")
            raise AlertTransientError("transient database error") from exc
        except SQLAlchemyError as exc:
            logger.exception("alert_get_database_error")
            raise AlertTransientError("database error") from exc

    def get_alert_by_anomaly(self, anomaly_id: str) -> Alert | None:
        """Get the most recent non-resolved alert for an anomaly.

        Args:
            anomaly_id: Anomaly UUID as string

        Returns:
            Alert instance or None if not found

        Raises:
            AlertTransientError: If query fails
        """
        try:
            with self._session_factory() as session:
                alert = session.execute(
                    select(Alert)
                    .where(
                        and_(
                            Alert.anomaly_id == anomaly_id,
                            Alert.status != AlertStatus.RESOLVED,
                        )
                    )
                    .order_by(Alert.triggered_at.desc())
                    .limit(1)
                ).scalar_one_or_none()

                return alert

        except OperationalError as exc:
            logger.exception("alert_get_by_anomaly_transient_error")
            raise AlertTransientError("transient database error") from exc
        except SQLAlchemyError as exc:
            logger.exception("alert_get_by_anomaly_database_error")
            raise AlertTransientError("database error") from exc

    # ------------------------------------------------------------------
    # Alert creation with deduplication
    # ------------------------------------------------------------------

    def create_alert(
        self,
        anomaly_id: UUID,
        inventory_id: UUID,
        severity: AlertSeverity,
        title: str,
        description: str | None = None,
        event_id: str | None = None,
    ) -> Alert:
        """Create a new alert with deduplication.

        Deduplicates alerts for the same anomaly within the configured time window.

        Args:
            anomaly_id: Associated anomaly ID
            inventory_id: Associated inventory ID
            severity: Alert severity (HIGH or CRITICAL)
            title: Alert title
            description: Optional alert description
            event_id: Optional source event ID

        Returns:
            Created or existing alert instance

        Raises:
            AlertTransientError: If database operation fails
        """
        try:
            with self._session_factory() as session:
                # Check for existing non-resolved alert within deduplication window
                dedup_window_seconds = self._settings.alert_deduplication_window_seconds
                cutoff_time = datetime.now(UTC) - timedelta(seconds=dedup_window_seconds)

                existing_alert = session.execute(
                    select(Alert)
                    .where(
                        and_(
                            Alert.anomaly_id == anomaly_id,
                            Alert.status != AlertStatus.RESOLVED,
                            Alert.triggered_at >= cutoff_time,
                        )
                    )
                    .order_by(Alert.triggered_at.desc())
                    .limit(1)
                ).scalar_one_or_none()

                if existing_alert:
                    logger.info(
                        "alert_deduplicated",
                        extra={
                            "anomaly_id": str(anomaly_id),
                            "existing_alert_id": str(existing_alert.id),
                            "dedup_window_seconds": dedup_window_seconds,
                        },
                    )
                    return existing_alert

                # Create new alert
                alert = Alert(
                    anomaly_id=anomaly_id,
                    inventory_id=inventory_id,
                    event_id=event_id,
                    severity=severity,
                    status=AlertStatus.TRIGGERED,
                    title=title,
                    description=description,
                )

                session.add(alert)
                session.commit()
                session.refresh(alert)

                logger.info(
                    "alert_created",
                    extra={
                        "alert_id": str(alert.id),
                        "anomaly_id": str(anomaly_id),
                        "severity": severity.value,
                    },
                )

                return alert

        except OperationalError as exc:
            logger.exception("alert_create_transient_error")
            raise AlertTransientError("transient database error") from exc
        except SQLAlchemyError as exc:
            logger.exception("alert_create_database_error")
            raise AlertTransientError("database error") from exc

    # ------------------------------------------------------------------
    # Alert lifecycle mutations
    # ------------------------------------------------------------------

    def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> Alert:
        """Acknowledge an alert.

        Args:
            alert_id: Alert ID
            acknowledged_by: User or system acknowledging the alert

        Returns:
            Updated alert instance

        Raises:
            AlertNotFoundError: If alert not found
            AlertTransientError: If operation fails
        """
        try:
            with self._session_factory() as session:
                alert = session.execute(
                    select(Alert).where(Alert.id == alert_id)
                ).scalar_one_or_none()

                if not alert:
                    raise AlertNotFoundError(f"alert {alert_id} not found")

                alert.status = AlertStatus.ACKNOWLEDGED
                alert.acknowledged_at = datetime.now(UTC)
                alert.acknowledged_by = acknowledged_by

                session.commit()
                session.refresh(alert)

                logger.info(
                    "alert_acknowledged",
                    extra={
                        "alert_id": str(alert.id),
                        "acknowledged_by": acknowledged_by,
                    },
                )

                return alert

        except AlertNotFoundError:
            raise
        except OperationalError as exc:
            logger.exception("alert_acknowledge_transient_error")
            raise AlertTransientError("transient database error") from exc
        except SQLAlchemyError as exc:
            logger.exception("alert_acknowledge_database_error")
            raise AlertTransientError("database error") from exc

    def resolve_alert(self, alert_id: str) -> Alert:
        """Resolve an alert.

        Args:
            alert_id: Alert ID

        Returns:
            Updated alert instance

        Raises:
            AlertNotFoundError: If alert not found
            AlertTransientError: If operation fails
        """
        try:
            with self._session_factory() as session:
                alert = session.execute(
                    select(Alert).where(Alert.id == alert_id)
                ).scalar_one_or_none()

                if not alert:
                    raise AlertNotFoundError(f"alert {alert_id} not found")

                alert.status = AlertStatus.RESOLVED
                alert.resolved_at = datetime.now(UTC)

                session.commit()
                session.refresh(alert)

                logger.info(
                    "alert_resolved",
                    extra={"alert_id": str(alert.id)},
                )

                return alert

        except AlertNotFoundError:
            raise
        except OperationalError as exc:
            logger.exception("alert_resolve_transient_error")
            raise AlertTransientError("transient database error") from exc
        except SQLAlchemyError as exc:
            logger.exception("alert_resolve_database_error")
            raise AlertTransientError("database error") from exc

    def suppress_alert(self, alert_id: str, suppressed_until: datetime) -> Alert:
        """Suppress an alert until a specified time.

        Args:
            alert_id: Alert ID
            suppressed_until: Timestamp until which to suppress

        Returns:
            Updated alert instance

        Raises:
            AlertNotFoundError: If alert not found
            AlertTransientError: If operation fails
        """
        try:
            with self._session_factory() as session:
                alert = session.execute(
                    select(Alert).where(Alert.id == alert_id)
                ).scalar_one_or_none()

                if not alert:
                    raise AlertNotFoundError(f"alert {alert_id} not found")

                alert.status = AlertStatus.SUPPRESSED
                alert.suppressed_until = suppressed_until

                session.commit()
                session.refresh(alert)

                logger.info(
                    "alert_suppressed",
                    extra={
                        "alert_id": str(alert.id),
                        "suppressed_until": suppressed_until.isoformat(),
                    },
                )

                return alert

        except AlertNotFoundError:
            raise
        except OperationalError as exc:
            logger.exception("alert_suppress_transient_error")
            raise AlertTransientError("transient database error") from exc
        except SQLAlchemyError as exc:
            logger.exception("alert_suppress_database_error")
            raise AlertTransientError("database error") from exc

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_alert_stats(self) -> AlertStatsData:
        """Get alert statistics.

        Returns:
            AlertStatsData with various statistics

        Raises:
            AlertTransientError: If query fails
        """
        try:
            with self._session_factory() as session:
                # Total count
                total = session.execute(select(func.count()).select_from(Alert)).scalar() or 0

                # Count by status
                triggered = (
                    session.execute(
                        select(func.count())
                        .select_from(Alert)
                        .where(Alert.status == AlertStatus.TRIGGERED)
                    ).scalar()
                    or 0
                )
                acknowledged = (
                    session.execute(
                        select(func.count())
                        .select_from(Alert)
                        .where(Alert.status == AlertStatus.ACKNOWLEDGED)
                    ).scalar()
                    or 0
                )
                resolved = (
                    session.execute(
                        select(func.count())
                        .select_from(Alert)
                        .where(Alert.status == AlertStatus.RESOLVED)
                    ).scalar()
                    or 0
                )
                suppressed = (
                    session.execute(
                        select(func.count())
                        .select_from(Alert)
                        .where(Alert.status == AlertStatus.SUPPRESSED)
                    ).scalar()
                    or 0
                )

                # Count by severity
                critical = (
                    session.execute(
                        select(func.count())
                        .select_from(Alert)
                        .where(Alert.severity == AlertSeverity.CRITICAL)
                    ).scalar()
                    or 0
                )
                high = (
                    session.execute(
                        select(func.count())
                        .select_from(Alert)
                        .where(Alert.severity == AlertSeverity.HIGH)
                    ).scalar()
                    or 0
                )

                # Average time to acknowledge (in seconds)
                avg_ack_time = session.execute(
                    select(
                        func.avg(
                            func.extract(
                                "epoch",
                                Alert.acknowledged_at - Alert.triggered_at,
                            )
                        )
                    )
                    .select_from(Alert)
                    .where(Alert.acknowledged_at.isnot(None))
                ).scalar()

                # Average time to resolve (in seconds)
                avg_resolution_time = session.execute(
                    select(
                        func.avg(
                            func.extract(
                                "epoch",
                                Alert.resolved_at - Alert.triggered_at,
                            )
                        )
                    )
                    .select_from(Alert)
                    .where(Alert.resolved_at.isnot(None))
                ).scalar()

                return AlertStatsData(
                    total=total,
                    triggered=triggered,
                    acknowledged=acknowledged,
                    resolved=resolved,
                    suppressed=suppressed,
                    critical=critical,
                    high=high,
                    avg_ack_time=float(avg_ack_time) if avg_ack_time else None,
                    avg_resolution_time=float(avg_resolution_time)
                    if avg_resolution_time
                    else None,
                )

        except OperationalError as exc:
            logger.exception("alert_stats_transient_error")
            raise AlertTransientError("transient database error") from exc
        except SQLAlchemyError as exc:
            logger.exception("alert_stats_database_error")
            raise AlertTransientError("database error") from exc


def get_alert_service() -> AlertService:
    """Dependency injection provider for AlertService.

    Returns:
        AlertService instance
    """
    return AlertService(session_factory=SessionLocal)
