from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.api.v1.endpoints.reconciliation import get_reconciliation_service
from app.database.models import ReconciliationStatus
from app.main import app
from app.services.reconciliation_service import (
    ReconciliationResult,
    ReconciliationService,
    ReconciliationTransientError,
)

client = TestClient(app)


def _make_result(
    *,
    store_id: str = "STORE-NYC",
    product_id: str = "SKU-100",
    expected: int = 42,
    actual: int = 40,
    difference: int = -2,
    status: ReconciliationStatus = ReconciliationStatus.MISMATCH,
) -> ReconciliationResult:
    return ReconciliationResult(
        store_id=store_id,
        product_id=product_id,
        expected_quantity=expected,
        actual_quantity=actual,
        difference=difference,
        status=status,
        reconciled_at=datetime(2026, 7, 17, 10, 0, 0, tzinfo=UTC),
    )


class TestGetReconciliationEndpoint:
    def test_returns_200_with_reconciliation_result(self) -> None:
        stub = MagicMock(spec=ReconciliationService)
        stub.reconcile.return_value = _make_result()

        app.dependency_overrides[get_reconciliation_service] = lambda: stub

        response = client.get("/api/v1/reconciliation/STORE-NYC/SKU-100")

        app.dependency_overrides.clear()

        assert response.status_code == 200
        body = response.json()
        assert body["store_id"] == "STORE-NYC"
        assert body["product_id"] == "SKU-100"
        assert body["expected_quantity"] == 42
        assert body["actual_quantity"] == 40
        assert body["difference"] == -2
        assert body["status"] == "mismatch"
        stub.reconcile.assert_called_once_with(store_id="STORE-NYC", product_id="SKU-100")

    def test_returns_match_status(self) -> None:
        stub = MagicMock(spec=ReconciliationService)
        stub.reconcile.return_value = _make_result(
            expected=10, actual=10, difference=0, status=ReconciliationStatus.MATCH
        )

        app.dependency_overrides[get_reconciliation_service] = lambda: stub

        response = client.get("/api/v1/reconciliation/STORE-A/SKU-200")

        app.dependency_overrides.clear()

        assert response.status_code == 200
        assert response.json()["status"] == "match"
        assert response.json()["difference"] == 0

    def test_returns_missing_status_when_no_inventory(self) -> None:
        stub = MagicMock(spec=ReconciliationService)
        stub.reconcile.return_value = _make_result(
            expected=0,
            actual=0,
            difference=0,
            status=ReconciliationStatus.MISSING,
        )

        app.dependency_overrides[get_reconciliation_service] = lambda: stub

        response = client.get("/api/v1/reconciliation/STORE-A/SKU-MISSING")

        app.dependency_overrides.clear()

        assert response.status_code == 200
        assert response.json()["status"] == "missing"

    def test_returns_503_on_transient_error(self) -> None:
        stub = MagicMock(spec=ReconciliationService)
        stub.reconcile.side_effect = ReconciliationTransientError("db down")

        app.dependency_overrides[get_reconciliation_service] = lambda: stub

        response = client.get("/api/v1/reconciliation/STORE-A/SKU-100")

        app.dependency_overrides.clear()

        assert response.status_code == 503

    def test_response_contains_reconciled_at_timestamp(self) -> None:
        stub = MagicMock(spec=ReconciliationService)
        stub.reconcile.return_value = _make_result()

        app.dependency_overrides[get_reconciliation_service] = lambda: stub

        response = client.get("/api/v1/reconciliation/STORE-NYC/SKU-100")

        app.dependency_overrides.clear()

        assert "reconciled_at" in response.json()

    def test_passes_path_params_to_service(self) -> None:
        stub = MagicMock(spec=ReconciliationService)
        stub.reconcile.return_value = _make_result(
            store_id="WAREHOUSE-42", product_id="SKU-XYZ"
        )

        app.dependency_overrides[get_reconciliation_service] = lambda: stub

        client.get("/api/v1/reconciliation/WAREHOUSE-42/SKU-XYZ")

        app.dependency_overrides.clear()

        stub.reconcile.assert_called_once_with(
            store_id="WAREHOUSE-42", product_id="SKU-XYZ"
        )
