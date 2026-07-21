from typing import Protocol

from app.cache.redis_client import ping_redis
from app.config.settings import get_settings
from app.database.session import check_database_connection
from app.schemas.health import HealthResponse


class DatabaseHealthChecker(Protocol):
    def is_available(self) -> bool: ...


class RedisHealthChecker(Protocol):
    def is_available(self) -> bool: ...


class SqlAlchemyDatabaseHealthChecker:
    def is_available(self) -> bool:
        return check_database_connection()


class DefaultRedisHealthChecker:
    def is_available(self) -> bool:
        return ping_redis()


class HealthService:
    def __init__(
        self,
        database_checker: DatabaseHealthChecker | None = None,
        redis_checker: RedisHealthChecker | None = None,
    ) -> None:
        self._database_checker = database_checker or SqlAlchemyDatabaseHealthChecker()
        self._redis_checker = redis_checker or DefaultRedisHealthChecker()

    async def check(self) -> HealthResponse:
        settings = get_settings()
        details: dict[str, str] = {"api": "ok"}

        details["postgres"] = "ok" if self._database_checker.is_available() else "unavailable"

        if settings.enable_dependency_health_checks:
            details["redis"] = self._check_redis()
        else:
            details["redis"] = "skipped"

        return self._build_health_response(details=details)

    async def check_liveness(self) -> HealthResponse:
        return self._build_health_response(details={"api": "ok"})

    async def check_readiness(self) -> HealthResponse:
        settings = get_settings()
        details: dict[str, str] = {
            "api": "ok",
            "postgres": "ok" if self._database_checker.is_available() else "unavailable",
        }

        if settings.enable_dependency_health_checks:
            details["redis"] = self._check_redis()
        else:
            details["redis"] = "skipped"

        return self._build_health_response(details=details)

    def _build_health_response(self, details: dict[str, str]) -> HealthResponse:
        settings = get_settings()
        status = "ok" if all(value in {"ok", "skipped"} for value in details.values()) else "degraded"
        return HealthResponse(
            status=status,
            service=settings.app_name,
            version=settings.app_version,
            details=details,
        )

    def _check_redis(self) -> str:
        try:
            return "ok" if self._redis_checker.is_available() else "unavailable"
        except Exception:
            return "unavailable"
