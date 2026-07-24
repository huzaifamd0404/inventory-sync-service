# Inventory Sync Service

Production-ready FastAPI backend for real-time inventory synchronization. Week 1 foundation includes API ingestion, Kafka publish/consume flow, PostgreSQL persistence, Redis cache synchronization, health checks, linting/format tooling, and CI.

## Architecture

The service follows Clean Architecture boundaries and SOLID-friendly module ownership:

- `app/api`: Transport layer (FastAPI routers, dependency wiring, error mapping).
- `app/services`: Application use cases (`InventoryEventService`, `InventoryService`, `SalesService`, `HealthService`, `ReconciliationService`).
- `app/producer` and `app/consumer`: Kafka adapters isolated from business logic.
- `app/database`: SQLAlchemy engine/session and persistence models.
- `app/cache`: Redis adapter.
- `app/schemas`: Pydantic contracts for request/response and docs.
- `app/config`: Settings and JSON structured logging.

**Inventory event flow:**

1. `POST /api/v1/inventory/events` validates payload and publishes to Kafka.
2. Consumer reads Kafka message and executes `InventoryService.process_event`.
3. Inventory state and history are written to PostgreSQL.
4. Current inventory snapshot is synchronized to Redis.

**Sales event flow:**

1. `POST /api/v1/sales/events` validates payload and publishes to the `sales_events` Kafka topic.
2. `KafkaSalesConsumer` reads the message and executes `SalesService.process_event`.
3. Sales record is persisted in PostgreSQL; inventory quantity is atomically deducted in the same transaction.
4. `GET /api/v1/sales/{store_id}/{product_id}` returns the aggregated sales summary.

## Project Structure

```text
inventory-sync-service/
  app/
    api/
    cache/
    config/
    consumer/
    database/
    producer/
    schemas/
    services/
    main.py
  tests/
  docs/
  docker-compose.yml
  Dockerfile
  pyproject.toml
  requirements.txt
```

## Quick Start (Docker)

1. Create env file:
   - PowerShell: `Copy-Item .env.example .env`
   - Bash: `cp .env.example .env`
2. Start API + consumer + dependencies:
   - `docker compose up --build`
3. Open:
   - API root: `http://localhost:8000/`
  - Health: `http://localhost:8000/health`
  - Liveness: `http://localhost:8000/health/live`
  - Readiness: `http://localhost:8000/health/ready`
  - Prometheus: `http://localhost:8000/metrics`
   - Swagger: `http://localhost:8000/docs`

## Local Development

1. Create and activate a Python 3.12 virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Copy env file:
   - `Copy-Item .env.example .env`
4. Run API:
   - `uvicorn app.main:app --reload --host 0.0.0.0 --port 8000`
5. Run consumer in another terminal:
   - `python -m app.consumer.worker`

## API Usage

### Publish Inventory Event

- Endpoint: `POST /api/v1/inventory/events`
- Request body example:

```json
{
  "product_id": "SKU-100",
  "store_id": "STORE-NYC",
  "operation": "RESTOCK",
  "quantity": 10,
  "timestamp": "2026-07-13T10:00:00Z"
}
```

- Response (`202 Accepted`) example:

```json
{
  "event_id": "0e9f4d70-98a3-41f3-b9bc-7439f4ac0f57"
}
```

### Publish Sales Event

Records a sales transaction and publishes it to the `sales_events` Kafka topic for downstream
persistence by the sales consumer.  The `sale_id` field is an **idempotency key** — submitting the
same `sale_id` twice has no effect and returns the same response.

- Endpoint: `POST /api/v1/sales/events`
- Request body example:

```json
{
  "sale_id": "ORDER-20260722-001",
  "product_id": "SKU-100",
  "store_id": "STORE-NYC",
  "quantity_sold": 5,
  "sale_price": "29.99",
  "timestamp": "2026-07-22T10:00:00Z"
}
```

- Response (`202 Accepted`) example:

```json
{
  "event_id": "0e9f4d70-98a3-41f3-b9bc-7439f4ac0f57",
  "sale_id": "ORDER-20260722-001"
}
```

### Get Sales Summary

Returns the aggregated sales summary for a product/store pair, including individual transaction
records ordered by most recent first.

- Endpoint: `GET /api/v1/sales/{store_id}/{product_id}`
- Response (`200 OK`) example:

```json
{
  "product_id": "SKU-100",
  "store_id": "STORE-NYC",
  "total_quantity_sold": 42,
  "transaction_count": 10,
  "total_revenue": "1259.58",
  "sales": [
    {
      "id": "a1b2c3d4-...",
      "inventory_id": "e5f6g7h8-...",
      "quantity_sold": 5,
      "sale_price": "29.99",
      "external_sale_id": "ORDER-20260722-001",
      "sold_at": "2026-07-22T10:00:00Z"
    }
  ]
}
```

Returns `404` when no inventory record exists for the given `store_id`/`product_id` pair.

### Reconcile Inventory

Derives the expected quantity from the full audit history (`inventory_history` deltas), compares it
with the live `inventory` snapshot, and returns the reconciliation result.
A new record is written to `reconciliation_records` **only** when the status or difference changes.

- Endpoint: `GET /api/v1/reconciliation/{store_id}/{product_id}`
- Response (`200 OK`) example:

```json
{
  "store_id": "STORE-NYC",
  "product_id": "SKU-100",
  "expected_quantity": 42,
  "actual_quantity": 40,
  "difference": -2,
  "status": "mismatch",
  "reconciled_at": "2026-07-17T10:00:00Z"
}
```

Possible `status` values:

| Value      | Meaning                                                        |
|------------|----------------------------------------------------------------|
| `match`    | Actual quantity equals the sum of all history deltas.          |
| `mismatch` | Actual quantity diverges from history-derived expectation.     |
| `missing`  | No inventory record exists for the given store/product pair.   |

### Anomaly Detection

The service includes a production-ready rule-based anomaly detection engine that automatically monitors
inventory and sales events for suspicious patterns. Anomalies are detected during event processing and
can be queried via REST APIs.

#### Detected Anomaly Types

- **Negative Inventory**: Inventory quantities below zero
- **Sudden Sales Spike**: Unusually large sales transactions compared to historical average
- **Large Inventory Adjustment**: Unusually large inventory adjustments
- **Rapid Consecutive Sales**: Multiple sales transactions in a short time window

#### Anomaly Query Examples

List all open anomalies:
```bash
curl "http://localhost:8000/api/v1/anomalies?status=open"
```

Get critical severity anomalies:
```bash
curl "http://localhost:8000/api/v1/anomalies?severity=critical"
```

Get anomalies for a specific inventory:
```bash
curl "http://localhost:8000/api/v1/anomalies/inventory/{inventory_id}"
```

Update anomaly status:
```bash
curl -X PATCH \
  "http://localhost:8000/api/v1/anomalies/{anomaly_id}/status" \
  -H "Content-Type: application/json" \
  -d '{"status": "resolved"}'
```

Get anomaly statistics:
```bash
curl "http://localhost:8000/api/v1/anomalies/stats/summary"
```

For comprehensive anomaly detection documentation, see [`docs/anomaly_detection.md`](docs/anomaly_detection.md).

### Real-Time Anomaly Alerting System

The service includes a production-ready alerting system that automatically generates and manages alerts 
when HIGH or CRITICAL severity anomalies are detected. Alerts are published to Kafka in real-time and can 
be managed via REST APIs.

#### Alert Features

- **Automatic Generation**: Alerts triggered by HIGH and CRITICAL severity anomalies
- **Intelligent Deduplication**: Prevents duplicate alerts within configurable time windows (default: 5 minutes)
- **Alert Lifecycle Management**: Track alerts from triggered through acknowledged to resolved
- **Real-Time Publishing**: Alerts published to `inventory_alerts` Kafka topic
- **Comprehensive REST APIs**: List, retrieve, acknowledge, resolve, and suppress alerts
- **Built-in Statistics**: Monitor alert volume, response times, and resolution metrics

#### Alert Status Flow

```
TRIGGERED → ACKNOWLEDGED → RESOLVED
   ↓
SUPPRESSED (can transition back to TRIGGERED after suppression expires)
```

#### Alert Management Examples

List all triggered CRITICAL alerts:
```bash
curl "http://localhost:8000/api/v1/alerts?severity=critical&status=triggered"
```

Get a specific alert:
```bash
curl "http://localhost:8000/api/v1/alerts/{alert_id}"
```

Acknowledge an alert:
```bash
curl -X POST \
  "http://localhost:8000/api/v1/alerts/{alert_id}/acknowledge" \
  -H "Content-Type: application/json" \
  -d '{"acknowledged_by": "operator@example.com"}'
```

Resolve an alert:
```bash
curl -X POST \
  "http://localhost:8000/api/v1/alerts/{alert_id}/resolve" \
  -H "Content-Type: application/json" \
  -d '{"resolved_by": "operator@example.com"}'
```

Suppress an alert until a specific time:
```bash
curl -X POST \
  "http://localhost:8000/api/v1/alerts/{alert_id}/suppress" \
  -H "Content-Type: application/json" \
  -d '{"suppressed_until": "2026-07-24T14:30:00Z"}'
```

Get alert statistics:
```bash
curl "http://localhost:8000/api/v1/alerts/stats/summary"
```

#### Alert Configuration

Configure alert behavior in `.env`:
```bash
# Enable/disable alerts by severity
ALERT_HIGH_SEVERITY_ENABLED=true
ALERT_CRITICAL_SEVERITY_ENABLED=true

# Deduplication time window (seconds)
ALERT_DEDUPLICATION_WINDOW_SECONDS=300

# Timeout for stale unacknowledged alerts (seconds)
ALERT_ACKNOWLEDGE_TIMEOUT_SECONDS=3600

# Kafka topic for alerts
KAFKA_TOPIC_ALERTS=inventory_alerts
```

For comprehensive alerting system documentation, see [`docs/alerting_system.md`](docs/alerting_system.md).

## Testing

- Unit and component tests:
  - `pytest -q`
- Full integration flow test (FastAPI -> Kafka -> PostgreSQL -> Redis):
  - PowerShell:
    - `$env:RUN_E2E_INTEGRATION_TESTS='true'; pytest -q -m integration`
  - Bash:
    - `RUN_E2E_INTEGRATION_TESTS=true pytest -q -m integration`

Integration tests require Kafka, PostgreSQL, and Redis to be reachable.

## Code Quality and Hooks

- Lint: `ruff check .`
- Format check: `black --check .`
- Import order: `isort --check-only .`
- Pre-commit install: `pre-commit install`
- Run all hooks: `pre-commit run --all-files`

## Database Migrations (Alembic)

- Create migration: `alembic revision --autogenerate -m "init"`
- Apply migration: `alembic upgrade head`
- Rollback one step: `alembic downgrade -1`

## Observability and Resilience

- JSON structured logs with request and trace correlation (`x-request-id`, `x-trace-id`).
- Structured event fields include `event_id`, `product_id`, and `processing_time` for ingestion and consumer logs.
- Prometheus metrics endpoint at `GET /metrics`.
- Business metrics include processed, failed, duplicate, retried, and DLQ events plus processing duration histogram.
- Batch metrics API remains available at `GET /api/v1/metrics`.
- Health probes include `GET /health`, `GET /health/live`, and `GET /health/ready` (returns `503` when not ready).
- Global exception handlers return consistent error payloads.
- Consumer resilience includes configurable exponential backoff retries for transient failures.
- Permanently failed events are persisted in `failed_events` and published to the `inventory_dlq` Kafka topic.
- Kafka offsets are committed only after successful processing or successful DLQ publishing.
- Consumer worker now handles `SIGINT`/`SIGTERM` and stops gracefully.
- Docker Compose health checks for API/PostgreSQL/Redis/Kafka/ZooKeeper.
- Persistent volumes enabled for PostgreSQL, Redis, Kafka, and ZooKeeper.

## CI

GitHub Actions workflow (`.github/workflows/ci.yml`) performs:

1. Dependency installation.
2. Startup of Kafka/PostgreSQL/Redis using Docker Compose.
3. Lint checks (Ruff, Black, isort).
4. Test execution including the integration pipeline test.
