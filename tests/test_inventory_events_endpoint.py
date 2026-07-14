from uuid import UUID

from fastapi.testclient import TestClient

from app.api.v1.endpoints.inventory_events import get_inventory_event_service
from app.main import app
from app.schemas.inventory import InventoryEvent

client = TestClient(app)


class StubInventoryEventService:
    def __init__(self) -> None:
        self.published_event: InventoryEvent | None = None

    async def publish_event(self, event: InventoryEvent) -> None:
        self.published_event = event


def test_publish_inventory_event_endpoint_returns_event_id() -> None:
    stub_service = StubInventoryEventService()
    app.dependency_overrides[get_inventory_event_service] = lambda: stub_service

    response = client.post(
        "/api/v1/inventory/events",
        json={
            "product_id": "SKU-55",
            "store_id": "STORE-NYC",
            "operation": "MANUAL_ADJUSTMENT",
            "quantity": 5,
            "timestamp": "2026-07-09T11:00:00Z",
        },
    )

    app.dependency_overrides.clear()

    assert response.status_code == 202
    payload = response.json()

    parsed_event_id = UUID(payload["event_id"])
    assert str(parsed_event_id) == payload["event_id"]
    assert stub_service.published_event is not None
    assert stub_service.published_event.product_id == "SKU-55"
    assert stub_service.published_event.store_id == "STORE-NYC"
    assert stub_service.published_event.operation.value == "MANUAL_ADJUSTMENT"
    assert str(stub_service.published_event.event_id) == payload["event_id"]


def test_publish_inventory_event_rejects_invalid_identifier_format() -> None:
    stub_service = StubInventoryEventService()
    app.dependency_overrides[get_inventory_event_service] = lambda: stub_service

    response = client.post(
        "/api/v1/inventory/events",
        json={
            "product_id": "SKU 55",
            "store_id": "STORE-NYC",
            "operation": "RESTOCK",
            "quantity": 5,
            "timestamp": "2026-07-09T11:00:00Z",
        },
    )

    app.dependency_overrides.clear()

    assert response.status_code == 422
    assert stub_service.published_event is None


def test_publish_inventory_event_rejects_out_of_range_quantity() -> None:
    stub_service = StubInventoryEventService()
    app.dependency_overrides[get_inventory_event_service] = lambda: stub_service

    response = client.post(
        "/api/v1/inventory/events",
        json={
            "product_id": "SKU-55",
            "store_id": "STORE-NYC",
            "operation": "RESTOCK",
            "quantity": 1000001,
            "timestamp": "2026-07-09T11:00:00Z",
        },
    )

    app.dependency_overrides.clear()

    assert response.status_code == 422
    assert stub_service.published_event is None
