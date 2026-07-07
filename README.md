# Inventory Sync Service

Production-ready FastAPI backend for real-time inventory synchronization with anomaly-detection-ready architecture.

## Highlights

- FastAPI service with `/`, `/health`, and interactive API docs at `/docs`.
- Python 3.12, Pydantic v2, SQLAlchemy 2.x, Alembic migrations.
- Containerized stack with FastAPI, PostgreSQL, Redis, ZooKeeper, and Kafka.
- Clean Architecture-inspired layering to keep domain logic testable and decoupled.
- CI pipeline for linting and tests.

## Project Structure

```
inventory-sync-service/
  app/
    api/         # HTTP delivery layer (routes/controllers)
    services/    # Application business services/use-cases
    models/      # ORM/domain entities
    database/    # SQLAlchemy base/session and persistence setup
    schemas/     # Pydantic v2 request/response DTOs
    producer/    # Kafka producer adapters
    consumer/    # Kafka consumer adapters
    cache/       # Redis adapter layer
    config/      # App settings and logging
    utils/       # Shared utility functions
    main.py      # FastAPI application entrypoint
  alembic/       # Database migration scripts
  tests/         # Unit/integration tests
  docs/          # Technical documentation
  .github/workflows/ci.yml
  Dockerfile
  docker-compose.yml
  pyproject.toml
  requirements.txt
```

## Quick Start (Docker)

1. Copy environment variables:
   - PowerShell: `Copy-Item .env.example .env`
   - Bash: `cp .env.example .env`
2. Start all services:
   - `docker compose up --build`
3. Open:
   - API root: `http://localhost:8000/`
   - Health: `http://localhost:8000/health`
   - Docs: `http://localhost:8000/docs`

## Local Development

1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Copy env file:
   - `Copy-Item .env.example .env`
4. Run app:
   - `uvicorn app.main:app --reload --host 0.0.0.0 --port 8000`

## Database Migrations (Alembic)

- Create migration: `alembic revision --autogenerate -m "init"`
- Apply migration: `alembic upgrade head`
- Rollback one step: `alembic downgrade -1`

## Quality Standards

- Lint: `ruff check .`
- Format check: `black --check .`
- Import order check: `isort --check-only .`
- Tests: `pytest -q`

## Clean Architecture and SOLID Notes

- API layer depends on service interfaces/use-cases, not persistence details.
- Infrastructure adapters (Redis, Kafka, SQLAlchemy) are isolated in dedicated packages.
- Configuration is centralized and typed through Pydantic settings.
- Explicit type hints are used for maintainability and safer refactors.

## CI

GitHub Actions workflow at `.github/workflows/ci.yml` runs lint checks and tests on push/PR.
