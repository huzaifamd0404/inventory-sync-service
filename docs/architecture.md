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
