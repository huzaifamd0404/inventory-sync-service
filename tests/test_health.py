from fastapi.testclient import TestClient

from app.api.v1.endpoints.health import get_health_service
from app.main import app
from app.services.health_service import HealthService

client = TestClient(app)


class StubDatabaseChecker:
    def __init__(self, available: bool) -> None:
        self._available = available

    def is_available(self) -> bool:
        return self._available


class StubRedisChecker:
    def __init__(self, available: bool) -> None:
        self._available = available

    def is_available(self) -> bool:
        return self._available


def test_root() -> None:
    response = client.get("/")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "running"


def test_health() -> None:
    app.dependency_overrides[get_health_service] = lambda: HealthService(
        database_checker=StubDatabaseChecker(available=True),
        redis_checker=StubRedisChecker(available=True),
    )

    response = client.get("/health")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["details"]["postgres"] == "ok"


def test_health_live() -> None:
    response = client.get("/health/live")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["details"]["api"] == "ok"


def test_health_ready() -> None:
    app.dependency_overrides[get_health_service] = lambda: HealthService(
        database_checker=StubDatabaseChecker(available=True),
        redis_checker=StubRedisChecker(available=True),
    )

    response = client.get("/health/ready")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["details"]["postgres"] == "ok"


def test_health_degraded_when_postgres_unavailable() -> None:
    app.dependency_overrides[get_health_service] = lambda: HealthService(
        database_checker=StubDatabaseChecker(available=False),
        redis_checker=StubRedisChecker(available=True),
    )

    response = client.get("/health")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["details"]["postgres"] == "unavailable"


def test_health_ready_returns_503_when_postgres_unavailable() -> None:
    app.dependency_overrides[get_health_service] = lambda: HealthService(
        database_checker=StubDatabaseChecker(available=False),
        redis_checker=StubRedisChecker(available=True),
    )

    response = client.get("/health/ready")

    app.dependency_overrides.clear()

    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["details"]["postgres"] == "unavailable"
