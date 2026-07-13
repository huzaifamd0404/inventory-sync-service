# Architecture Overview

The service follows Clean Architecture boundaries:

- `app/api`: FastAPI routes and transport-level concerns.
- `app/services`: Application use cases and orchestration logic.
- `app/models`: Domain/ORM entities.
- `app/schemas`: Input/output DTOs (Pydantic v2).
- `app/database`: Persistence and DB session setup.
- `app/cache`: Redis adapter layer.
- `app/producer` and `app/consumer`: Kafka integration adapters.
- `app/config`: Configuration and logging.
- `app/utils`: Cross-cutting utility functions.

Core principles applied:

- SOLID-focused module separation.
- Dependency inversion through adapter modules.
- Explicit typing and schema validation at boundaries.

## Runtime Flow

1. API validates request payload (`InventoryEventCreate`) and emits event to Kafka.
2. Consumer receives event and invokes `InventoryService`.
3. Service performs idempotency check using source event id.
4. Inventory and inventory history are committed transactionally to PostgreSQL.
5. Latest inventory snapshot is written to Redis for low-latency reads.

## Reliability and Operations

- Retry policies are applied in producer and consumer adapters.
- Non-retryable domain rule failures are committed and skipped to avoid poison-message loops.
- Structured JSON logs include request and event context for observability.
- Health checks expose dependency status and are used by Docker Compose and CI startup gates.
