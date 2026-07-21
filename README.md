# Inventory Sync Service

Production-ready FastAPI backend for real-time inventory synchronization. Week 1 foundation includes API ingestion, Kafka publish/consume flow, PostgreSQL persistence, Redis cache synchronization, health checks, linting/format tooling, and CI.

## Architecture

The service follows Clean Architecture boundaries and SOLID-friendly module ownership:

- `app/api`: Transport layer (FastAPI routers, dependency wiring, error mapping).
- `app/services`: Application use cases (`InventoryEventService`, `InventoryService`, `HealthService`, `ReconciliationService`).
- `app/producer` and `app/consumer`: Kafka adapters isolated from business logic.
- `app/database`: SQLAlchemy engine/session and persistence models.
- `app/cache`: Redis adapter.
- `app/schemas`: Pydantic contracts for request/response and docs.
- `app/config`: Settings and JSON structured logging.

Event flow:

1. `POST /api/v1/inventory/events` validates payload and publishes to Kafka.
2. Consumer reads Kafka message and executes `InventoryService.process_event`.
3. Inventory state and history are written to PostgreSQL.
4. Current inventory snapshot is synchronized to Redis.

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
