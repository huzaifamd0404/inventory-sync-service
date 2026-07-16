from sqlalchemy import create_engine, inspect

from app.database.base import Base
from app.database.models import (
    Anomaly,
    AnomalySeverity,
    AnomalyStatus,
    Inventory,
    InventoryChangeType,
    InventoryHistory,
    Sales,
)


def test_create_all_contains_expected_tables() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    table_names = set(inspect(engine).get_table_names())
    assert {
        "anomalies",
        "failed_events",
        "inventory",
        "inventory_history",
        "processed_events",
        "sales",
    }.issubset(table_names)


def test_inventory_relationship_wiring() -> None:
    inventory = Inventory(sku="SKU-001", warehouse_id="WH-1", quantity=20, reorder_level=5)

    history = InventoryHistory(
        inventory=inventory,
        change_type=InventoryChangeType.SYNC,
        quantity_before=15,
        quantity_after=20,
        quantity_delta=5,
        source_event_id="evt-001",
    )
    sale = Sales(inventory=inventory, quantity_sold=2, external_sale_id="sale-001")
    anomaly = Anomaly(
        inventory=inventory,
        anomaly_type="abnormal_sales_spike",
        severity=AnomalySeverity.HIGH,
        status=AnomalyStatus.OPEN,
        score=0.92,
    )

    assert history.inventory is inventory
    assert sale.inventory is inventory
    assert anomaly.inventory is inventory
    assert inventory.history_entries[0] is history
    assert inventory.sales[0] is sale
    assert inventory.anomalies[0] is anomaly
