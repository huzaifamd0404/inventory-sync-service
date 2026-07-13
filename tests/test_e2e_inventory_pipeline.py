from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from kafka import KafkaConsumer
from sqlalchemy import delete, select

from app.cache.redis_client import get_redis_client
from app.config.settings import get_settings
from app.database.base import Base
from app.database.models import Inventory, InventoryHistory
from app.database.session import SessionLocal, engine
from app.main import app
from app.schemas.inventory import InventoryEvent
from app.services.inventory_service import InventoryService


@pytest.mark.integration
def test_e2e_inventory_pipeline_fastapi_to_kafka_to_postgres_to_redis() -> None:
    if os.getenv("RUN_E2E_INTEGRATION_TESTS", "false").lower() != "true":
        pytest.skip("Set RUN_E2E_INTEGRATION_TESTS=true to run full integration pipeline test")

    Base.metadata.create_all(bind=engine)
    settings = get_settings()

    product_id = f"SKU-E2E-{uuid4().hex[:8]}"
    store_id = f"STORE-E2E-{uuid4().hex[:8]}"
    group_id = f"inventory-e2e-{uuid4().hex}"

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/inventory/events",
            json={
                "product_id": product_id,
                "store_id": store_id,
                "operation": "RESTOCK",
                "quantity": 7,
                "timestamp": datetime.now(UTC).isoformat(),
            },
        )

    assert response.status_code == 202
    event_id = response.json()["event_id"]

    consumer = KafkaConsumer(
        settings.kafka_topic_inventory_updates,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
    )

    consumed_event: InventoryEvent | None = None
    try:
        for record in consumer:
            payload = record.value
            if payload.get("event_id") != event_id:
                continue
            consumed_event = InventoryEvent.model_validate(payload)
            consumer.commit()
            break
    finally:
        consumer.close()

    assert consumed_event is not None

    redis_client = get_redis_client()
    inventory_service = InventoryService(session_factory=SessionLocal, redis_client=redis_client)

    result = inventory_service.process_event(consumed_event)
    assert result.quantity_after == 7

    cache_key = f"inventory:{store_id}:{product_id}"
    cached_payload = redis_client.get(cache_key)
    assert cached_payload is not None

    cached_data = json.loads(cached_payload)
    assert cached_data["quantity"] == 7
    assert cached_data["source_event_id"] == event_id

    with SessionLocal() as session:
        inventory = session.execute(
            select(Inventory)
            .where(Inventory.sku == product_id)
            .where(Inventory.warehouse_id == store_id)
        ).scalar_one()
        history = session.execute(
            select(InventoryHistory).where(InventoryHistory.source_event_id == event_id)
        ).scalar_one()

        assert inventory.quantity == 7
        assert history.quantity_after == 7

        session.execute(
            delete(InventoryHistory).where(InventoryHistory.inventory_id == inventory.id)
        )
        session.execute(delete(Inventory).where(Inventory.id == inventory.id))
        session.commit()

    redis_client.delete(cache_key)
