from sqlalchemy import text

from app.cache.redis_client import ping_redis
from app.config.settings import get_settings
from app.database.session import SessionLocal
from app.schemas.health import HealthResponse


class HealthService:
    async def check(self) -> HealthResponse:
        settings = get_settings()
        details: dict[str, str] = {"api": "ok"}

        if settings.enable_dependency_health_checks:
            details["postgres"] = self._check_postgres()
            details["redis"] = self._check_redis()
        else:
            details["postgres"] = "skipped"
            details["redis"] = "skipped"

        status = "ok" if all(value in {"ok", "skipped"} for value in details.values()) else "degraded"
        return HealthResponse(
            status=status,
            service=settings.app_name,
            version=settings.app_version,
            details=details,
        )

    @staticmethod
    def _check_postgres() -> str:
        try:
            with SessionLocal() as session:
                session.execute(text("SELECT 1"))
            return "ok"
        except Exception:
            return "unavailable"

    @staticmethod
    def _check_redis() -> str:
        try:
            return "ok" if ping_redis() else "unavailable"
        except Exception:
            return "unavailable"
