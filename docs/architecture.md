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
2. Consumer receives event, validates it, and executes the use case through `RetryService`.
3. Service performs idempotency check using source event id.
4. Inventory and inventory history are committed transactionally to PostgreSQL.
5. Latest inventory snapshot is written to Redis for low-latency reads.
6. If retries are exhausted or processing is non-retryable, failure is persisted in `failed_events` and published to Kafka DLQ topic `inventory_dlq`.
7. Consumer commits offsets only after successful processing or successful DLQ publication.

## Reliability and Operations

- Retry policies are applied in producer and consumer adapters with configurable exponential backoff.
- Failed-event handling is durable: `failed_events` table persists context and retry metadata for operational replay.
- DLQ payloads include event metadata (`event_id`, `failure_reason`, `retry_count`, `timestamp`) and source Kafka coordinates.
- Structured JSON logs include request and event context for observability.
- Health checks expose dependency status and are used by Docker Compose and CI startup gates.
