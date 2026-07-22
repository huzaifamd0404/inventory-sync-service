from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.api.v1.endpoints.sales import get_sales_kafka_producer, get_sales_service
from app.database.base import Base
from app.database.models import Inventory
from app.database.session import get_db_session
from app.main import app
from app.schemas.sales import SalesSummaryResponse
from app.services.sales_service import SalesService

client = TestClient(app)

VALID_PAYLOAD = {
    "sale_id": "ORDER-20260722-001",
    "product_id": "SKU-100",
    "store_id": "STORE-NYC",
    "quantity_sold": 5,
    "sale_price": "29.99",
    "timestamp": "2026-07-22T10:00:00Z",
}


# ---------------------------------------------------------------------------
# Stubs / factories
# ---------------------------------------------------------------------------


def make_stub_producer(raises: Exception | None = None) -> MagicMock:
    stub = MagicMock()
    if raises:
        stub.publish_sales_event.side_effect = raises
    return stub


def make_in_memory_session_factory() -> sessionmaker[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def make_in_memory_session(sf: sessionmaker[Session]) -> Session:
    return sf()


# ---------------------------------------------------------------------------
# POST /api/v1/sales/events
# ---------------------------------------------------------------------------


def test_publish_sales_event_returns_202_with_event_id_and_sale_id() -> None:
    stub = make_stub_producer()
    app.dependency_overrides[get_sales_kafka_producer] = lambda: stub

    try:
        response = client.post("/api/v1/sales/events", json=VALID_PAYLOAD)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    body = response.json()
    UUID(body["event_id"])  # must be valid UUID
    assert body["sale_id"] == "ORDER-20260722-001"


def test_publish_sales_event_calls_producer_with_correct_data() -> None:
    stub = make_stub_producer()
    app.dependency_overrides[get_sales_kafka_producer] = lambda: stub

    try:
        client.post("/api/v1/sales/events", json=VALID_PAYLOAD)
    finally:
        app.dependency_overrides.clear()

    stub.publish_sales_event.assert_called_once()
    published_event = stub.publish_sales_event.call_args[0][0]
    assert published_event.sale_id == "ORDER-20260722-001"
    assert published_event.product_id == "SKU-100"
    assert published_event.store_id == "STORE-NYC"
    assert published_event.quantity_sold == 5


def test_publish_sales_event_returns_503_when_kafka_unavailable() -> None:
    from app.producer.sales_kafka_producer import KafkaSalesPublishError

    stub = make_stub_producer(raises=KafkaSalesPublishError("kafka down"))
    app.dependency_overrides[get_sales_kafka_producer] = lambda: stub

    try:
        response = client.post("/api/v1/sales/events", json=VALID_PAYLOAD)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503


def test_publish_sales_event_rejects_zero_quantity() -> None:
    stub = make_stub_producer()
    app.dependency_overrides[get_sales_kafka_producer] = lambda: stub

    try:
        response = client.post(
            "/api/v1/sales/events",
            json={**VALID_PAYLOAD, "quantity_sold": 0},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    stub.publish_sales_event.assert_not_called()


def test_publish_sales_event_rejects_negative_quantity() -> None:
    stub = make_stub_producer()
    app.dependency_overrides[get_sales_kafka_producer] = lambda: stub

    try:
        response = client.post(
            "/api/v1/sales/events",
            json={**VALID_PAYLOAD, "quantity_sold": -1},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422


def test_publish_sales_event_rejects_invalid_product_id_format() -> None:
    stub = make_stub_producer()
    app.dependency_overrides[get_sales_kafka_producer] = lambda: stub

    try:
        response = client.post(
            "/api/v1/sales/events",
            json={**VALID_PAYLOAD, "product_id": "SKU 100"},  # space not allowed
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422


def test_publish_sales_event_accepts_null_sale_price() -> None:
    stub = make_stub_producer()
    app.dependency_overrides[get_sales_kafka_producer] = lambda: stub

    try:
        response = client.post(
            "/api/v1/sales/events",
            json={**VALID_PAYLOAD, "sale_price": None},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202


def test_publish_sales_event_rejects_negative_sale_price() -> None:
    stub = make_stub_producer()
    app.dependency_overrides[get_sales_kafka_producer] = lambda: stub

    try:
        response = client.post(
            "/api/v1/sales/events",
            json={**VALID_PAYLOAD, "sale_price": "-1.00"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422


def test_publish_sales_event_rejects_missing_sale_id() -> None:
    stub = make_stub_producer()
    app.dependency_overrides[get_sales_kafka_producer] = lambda: stub
    payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "sale_id"}

    try:
        response = client.post("/api/v1/sales/events", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422


# ---------------------------------------------------------------------------
# GET /api/v1/sales/{store_id}/{product_id}
# ---------------------------------------------------------------------------


def _stub_service_returning(summary: SalesSummaryResponse | None) -> SalesService:
    """Create a SalesService stub whose get_sales_summary returns a fixed value."""
    stub = MagicMock(spec=SalesService)
    stub.get_sales_summary.return_value = summary
    return stub


def test_get_sales_summary_returns_200_with_summary() -> None:
    summary = SalesSummaryResponse(
        product_id="SKU-100",
        store_id="STORE-NYC",
        total_quantity_sold=10,
        transaction_count=2,
        total_revenue=Decimal("299.90"),
        sales=[],
    )
    stub_service = _stub_service_returning(summary)
    app.dependency_overrides[get_sales_service] = lambda: stub_service

    try:
        response = client.get("/api/v1/sales/STORE-NYC/SKU-100")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["product_id"] == "SKU-100"
    assert body["store_id"] == "STORE-NYC"
    assert body["total_quantity_sold"] == 10
    assert body["transaction_count"] == 2


def test_get_sales_summary_returns_404_when_product_not_found() -> None:
    stub_service = _stub_service_returning(None)
    app.dependency_overrides[get_sales_service] = lambda: stub_service

    try:
        response = client.get("/api/v1/sales/STORE-NYC/GHOST-SKU")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404


def test_get_sales_summary_passes_path_params_to_service() -> None:
    summary = SalesSummaryResponse(
        product_id="SKU-XYZ",
        store_id="STORE-LA",
        total_quantity_sold=0,
        transaction_count=0,
        total_revenue=None,
        sales=[],
    )
    stub_service = _stub_service_returning(summary)
    app.dependency_overrides[get_sales_service] = lambda: stub_service

    try:
        client.get("/api/v1/sales/STORE-LA/SKU-XYZ")
    finally:
        app.dependency_overrides.clear()

    stub_service.get_sales_summary.assert_called_once()
    _, product_id_arg, store_id_arg = stub_service.get_sales_summary.call_args[0]
    assert product_id_arg == "SKU-XYZ"
    assert store_id_arg == "STORE-LA"
