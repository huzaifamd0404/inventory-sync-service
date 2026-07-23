"""Service for querying and managing anomalies."""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import and_, func, select
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.database.models import Anomaly, AnomalySeverity, AnomalyStatus
from app.database.session import SessionLocal

logger = logging.getLogger(__name__)


class AnomalyServiceError(Exception):
    """Base exception for anomaly service failures."""


class AnomalyTransientError(AnomalyServiceError):
    """Raised for transient failures that can be retried safely."""


class AnomalyNotFoundError(AnomalyServiceError):
    """Raised when requested anomaly is not found."""


@dataclass(frozen=True)
class AnomalyQueryResult:
    """Result from anomaly query."""

    anomalies: list[Anomaly]
    total: int


class AnomalyService:
    """Service for anomaly queries and mutations."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    # ------------------------------------------------------------------
    # Query operations
    # ------------------------------------------------------------------

    def list_anomalies(
        self,
        skip: int = 0,
        limit: int = 20,
        inventory_id: str | None = None,
        anomaly_type: str | None = None,
        severity: AnomalySeverity | None = None,
        status: AnomalyStatus | None = None,
    ) -> AnomalyQueryResult:
        """List anomalies with optional filtering and pagination.

        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            inventory_id: Filter by inventory ID
            anomaly_type: Filter by anomaly type
            severity: Filter by severity level
            status: Filter by status

        Returns:
            AnomalyQueryResult with anomalies and total count

        Raises:
            AnomalyTransientError: If query fails
        """
        try:
            with self._session_factory() as session:
                # Build query with filters
                query = select(Anomaly)

                filters = []
                if inventory_id:
                    filters.append(Anomaly.inventory_id == inventory_id)
                if anomaly_type:
                    filters.append(Anomaly.anomaly_type == anomaly_type)
                if severity:
                    filters.append(Anomaly.severity == severity)
                if status:
                    filters.append(Anomaly.status == status)

                if filters:
                    query = query.where(and_(*filters))

                # Get total count
                total_query = select(func.count()).select_from(Anomaly)
                if filters:
                    total_query = total_query.where(and_(*filters))
                total = session.execute(total_query).scalar() or 0

                # Get paginated results
                anomalies = session.execute(
                    query.order_by(Anomaly.detected_at.desc()).offset(skip).limit(limit)
                ).scalars().all()

                logger.info(
                    "anomalies_listed",
                    extra={
                        "total": total,
                        "skip": skip,
                        "limit": limit,
                        "count": len(anomalies),
                    },
                )

                return AnomalyQueryResult(anomalies=anomalies, total=total)

        except OperationalError as exc:
            logger.exception("anomaly_service_operational_error")
            raise AnomalyTransientError("database operational error") from exc
        except SQLAlchemyError as exc:
            logger.exception("anomaly_service_database_error")
            raise AnomalyTransientError("database error") from exc

    def get_anomaly(self, anomaly_id: str) -> Anomaly:
        """Retrieve a specific anomaly by ID.

        Args:
            anomaly_id: UUID of the anomaly

        Returns:
            The Anomaly object

        Raises:
            AnomalyNotFoundError: If anomaly not found
            AnomalyTransientError: If query fails
        """
        try:
            with self._session_factory() as session:
                anomaly = session.get(Anomaly, anomaly_id)
                if anomaly is None:
                    logger.warning(
                        "anomaly_not_found",
                        extra={"anomaly_id": str(anomaly_id)},
                    )
                    raise AnomalyNotFoundError(f"Anomaly {anomaly_id} not found")

                logger.info("anomaly_retrieved", extra={"anomaly_id": str(anomaly_id)})
                return anomaly

        except AnomalyNotFoundError:
            raise
        except OperationalError as exc:
            logger.exception(
                "anomaly_service_operational_error",
                extra={"anomaly_id": str(anomaly_id)},
            )
            raise AnomalyTransientError("database operational error") from exc
        except SQLAlchemyError as exc:
            logger.exception(
                "anomaly_service_database_error",
                extra={"anomaly_id": str(anomaly_id)},
            )
            raise AnomalyTransientError("database error") from exc

    def get_anomalies_by_inventory(
        self,
        inventory_id: str,
        status: AnomalyStatus | None = None,
    ) -> list[Anomaly]:
        """Get all anomalies for a specific inventory.

        Args:
            inventory_id: UUID of the inventory
            status: Optional filter by status

        Returns:
            List of Anomaly objects

        Raises:
            AnomalyTransientError: If query fails
        """
        try:
            with self._session_factory() as session:
                query = select(Anomaly).where(Anomaly.inventory_id == inventory_id)

                if status:
                    query = query.where(Anomaly.status == status)

                anomalies = session.execute(
                    query.order_by(Anomaly.detected_at.desc())
                ).scalars().all()

                logger.info(
                    "anomalies_retrieved_by_inventory",
                    extra={
                        "inventory_id": str(inventory_id),
                        "count": len(anomalies),
                    },
                )

                return anomalies

        except OperationalError as exc:
            logger.exception(
                "anomaly_service_operational_error",
                extra={"inventory_id": str(inventory_id)},
            )
            raise AnomalyTransientError("database operational error") from exc
        except SQLAlchemyError as exc:
            logger.exception(
                "anomaly_service_database_error",
                extra={"inventory_id": str(inventory_id)},
            )
            raise AnomalyTransientError("database error") from exc

    def get_stats(self) -> dict:
        """Get statistics about anomalies.

        Returns:
            Dictionary with anomaly statistics

        Raises:
            AnomalyTransientError: If query fails
        """
        try:
            with self._session_factory() as session:
                # Total anomalies
                total = session.execute(
                    select(func.count()).select_from(Anomaly)
                ).scalar() or 0

                # Count by status
                open_count = session.execute(
                    select(func.count()).select_from(Anomaly)
                    .where(Anomaly.status == AnomalyStatus.OPEN)
                ).scalar() or 0

                investigating_count = session.execute(
                    select(func.count()).select_from(Anomaly)
                    .where(Anomaly.status == AnomalyStatus.INVESTIGATING)
                ).scalar() or 0

                resolved_count = session.execute(
                    select(func.count()).select_from(Anomaly)
                    .where(Anomaly.status == AnomalyStatus.RESOLVED)
                ).scalar() or 0

                # Count by severity
                critical_count = session.execute(
                    select(func.count()).select_from(Anomaly)
                    .where(Anomaly.severity == AnomalySeverity.CRITICAL)
                ).scalar() or 0

                high_count = session.execute(
                    select(func.count()).select_from(Anomaly)
                    .where(Anomaly.severity == AnomalySeverity.HIGH)
                ).scalar() or 0

                medium_count = session.execute(
                    select(func.count()).select_from(Anomaly)
                    .where(Anomaly.severity == AnomalySeverity.MEDIUM)
                ).scalar() or 0

                low_count = session.execute(
                    select(func.count()).select_from(Anomaly)
                    .where(Anomaly.severity == AnomalySeverity.LOW)
                ).scalar() or 0

                # Count by type
                type_counts = session.execute(
                    select(Anomaly.anomaly_type, func.count())
                    .group_by(Anomaly.anomaly_type)
                ).all()

                anomaly_types_count = {atype: count for atype, count in type_counts}

                stats = {
                    "total_anomalies": total,
                    "open_anomalies": open_count,
                    "investigating_anomalies": investigating_count,
                    "resolved_anomalies": resolved_count,
                    "critical_count": critical_count,
                    "high_count": high_count,
                    "medium_count": medium_count,
                    "low_count": low_count,
                    "anomaly_types_count": anomaly_types_count,
                }

                logger.info("anomaly_stats_retrieved", extra=stats)
                return stats

        except OperationalError as exc:
            logger.exception("anomaly_service_operational_error")
            raise AnomalyTransientError("database operational error") from exc
        except SQLAlchemyError as exc:
            logger.exception("anomaly_service_database_error")
            raise AnomalyTransientError("database error") from exc

    # ------------------------------------------------------------------
    # Mutation operations
    # ------------------------------------------------------------------

    def update_anomaly_status(
        self,
        anomaly_id: str,
        status: AnomalyStatus,
        resolved_at: datetime | None = None,
    ) -> Anomaly:
        """Update the status of an anomaly.

        Args:
            anomaly_id: UUID of the anomaly
            status: New status
            resolved_at: Optional timestamp when resolved

        Returns:
            Updated Anomaly object

        Raises:
            AnomalyNotFoundError: If anomaly not found
            AnomalyTransientError: If update fails
        """
        try:
            with self._session_factory() as session:
                with session.begin():
                    anomaly = session.get(Anomaly, anomaly_id)
                    if anomaly is None:
                        raise AnomalyNotFoundError(f"Anomaly {anomaly_id} not found")

                    anomaly.status = status
                    if status == AnomalyStatus.RESOLVED:
                        anomaly.resolved_at = resolved_at or datetime.now(UTC)

                    session.flush()
                    logger.info(
                        "anomaly_status_updated",
                        extra={
                            "anomaly_id": str(anomaly_id),
                            "status": status.value,
                        },
                    )

                    return anomaly

        except AnomalyNotFoundError:
            raise
        except OperationalError as exc:
            logger.exception(
                "anomaly_service_operational_error",
                extra={"anomaly_id": str(anomaly_id)},
            )
            raise AnomalyTransientError("database operational error") from exc
        except SQLAlchemyError as exc:
            logger.exception(
                "anomaly_service_database_error",
                extra={"anomaly_id": str(anomaly_id)},
            )
            raise AnomalyTransientError("database error") from exc


def get_anomaly_service() -> AnomalyService:
    """Dependency injection factory for AnomalyService.

    Returns:
        Configured AnomalyService instance
    """
    return AnomalyService(session_factory=SessionLocal)
