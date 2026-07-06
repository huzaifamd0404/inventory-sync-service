# Inventory Sync Service

Realtime inventory synchronization and anomaly detection service.

## Quick Start

1. Copy environment template:
   - `cp .env.example .env` (Linux/macOS)
   - `Copy-Item .env.example .env` (PowerShell)
2. Start with Docker:
   - `docker compose up --build`

## Local Development

1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Run the service entry point once implemented.

## Project Files

- `.env.example`: environment variable template.
- `requirements.txt`: Python dependencies.
- `Dockerfile`: image build definition.
- `docker-compose.yml`: local multi-container orchestration.
