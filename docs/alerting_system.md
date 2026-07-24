# Real-Time Anomaly Alerting System

## Overview

The Real-Time Anomaly Alerting System is a production-ready component of the Inventory Sync Service that automatically generates and manages alerts when the anomaly detection engine identifies HIGH or CRITICAL severity anomalies.

The system follows Clean Architecture principles, implements SOLID design patterns, and includes:
- **Automatic Alert Generation**: Triggered by anomaly detection
- **Intelligent Deduplication**: Prevents duplicate alerts within configurable time windows
- **Alert Lifecycle Management**: Track alert status from triggered through resolved
- **Kafka Integration**: Real-time alert publishing to `inventory_alerts` topic
- **REST APIs**: Complete management and querying capabilities
- **Structured Logging**: JSON formatted logs for monitoring and debugging
- **Comprehensive Testing**: Unit and integration tests with high coverage

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────────────┐
│                    Event Processing Pipeline                     │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │ Batch Processing Service    │
        │  - Event aggregation        │
        │  - Inventory processing     │
        └──────────┬──────────────────┘
                   │
         ┌─────────┴─────────┐
         ▼                   ▼
    ┌────────────┐   ┌──────────────┐
    │ Anomaly    │   │ Inventory    │
    │ Detection  │   │ Service      │
    │ Service    │   └──────────────┘
    └─────┬──────┘
          │ Detects HIGH/CRITICAL anomalies
          ▼
    ┌──────────────────┐
    │ Alert Service    │
    │ - Deduplication  │
    │ - Persistence    │
    │ - Lifecycle mgmt │
    └────────┬─────────┘
             │
        ┌────┴────┐
        ▼         ▼
    ┌────────┐ ┌──────────────┐
    │Database│ │Alert Kafka   │
    │(alerts)│ │ Producer     │
    └────────┘ └──────┬───────┘
                      │
                      ▼
            ┌──────────────────┐
            │ inventory_alerts │
            │ Kafka Topic      │
            └──────────────────┘
```

### Database Schema

**alerts table**:
```sql
CREATE TABLE alerts (
    id UUID PRIMARY KEY,
    anomaly_id UUID NOT NULL (FK: anomalies),
    inventory_id UUID NOT NULL (FK: inventory),
    event_id VARCHAR(128) UNIQUE,
    severity ENUM('high', 'critical') NOT NULL,
    status ENUM('triggered', 'acknowledged', 'resolved', 'suppressed') NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    triggered_at TIMESTAMP NOT NULL,
    acknowledged_at TIMESTAMP,
    acknowledged_by VARCHAR(255),
    resolved_at TIMESTAMP,
    suppressed_until TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- Indexes for performance
CREATE INDEX ix_alerts_anomaly_triggered_at ON alerts(anomaly_id, triggered_at);
CREATE INDEX ix_alerts_status_severity ON alerts(status, severity);
CREATE INDEX ix_alerts_inventory_triggered_at ON alerts(inventory_id, triggered_at);
CREATE UNIQUE INDEX ix_alerts_event_id ON alerts(event_id);
```

## Configuration

Alert behavior is configured in `app/config/settings.py`:

```python
# Alert deduplication window (seconds)
alert_deduplication_window_seconds: int = 300

# Enable/disable alerts by severity
alert_high_severity_enabled: bool = True
alert_critical_severity_enabled: bool = True

# Timeout for stale unacknowledged alerts
alert_acknowledge_timeout_seconds: int = 3600

# Kafka topic for alerts
kafka_topic_alerts: str = "inventory_alerts"
```

### Environment Variables

```bash
# Alert configuration
ALERT_DEDUPLICATION_WINDOW_SECONDS=300
ALERT_HIGH_SEVERITY_ENABLED=true
ALERT_CRITICAL_SEVERITY_ENABLED=true
ALERT_ACKNOWLEDGE_TIMEOUT_SECONDS=3600

# Kafka topic
KAFKA_TOPIC_ALERTS=inventory_alerts
```

## Alert Generation

### Trigger Conditions

Alerts are automatically generated when anomalies with the following severities are detected:
- **CRITICAL**: Database persisted issues (e.g., negative inventory)
- **HIGH**: Potential problems (e.g., unusual sales spikes)

Alerts are NOT generated for LOW or MEDIUM severity anomalies.

### Deduplication Strategy

The system implements an intelligent deduplication mechanism:

1. **Time Window**: Configured via `alert_deduplication_window_seconds` (default: 300 seconds)
2. **Per-Anomaly**: Deduplication is performed per anomaly ID
3. **Non-Resolved Status**: Only non-resolved alerts participate in deduplication
4. **Return Existing**: Returns existing alert if one exists within the window

**Example**:
```
T=0s:  Anomaly detected → Alert generated (ID: A1)
T=30s: Same anomaly again → Returns existing Alert A1 (deduplicated)
T=60s: Same anomaly again → Returns existing Alert A1 (deduplicated)
T=350s: Same anomaly again → New Alert generated (ID: A2) - outside window
```

### Alert Payload (Kafka)

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "anomaly_id": "660e8400-e29b-41d4-a716-446655440001",
  "inventory_id": "770e8400-e29b-41d4-a716-446655440002",
  "event_id": "evt-001",
  "severity": "critical",
  "status": "triggered",
  "title": "Alert: negative_inventory",
  "description": "Negative inventory detected for SKU-123 in warehouse WH-1",
  "triggered_at": "2026-07-24T10:30:45.123456+00:00",
  "acknowledged_at": null,
  "acknowledged_by": null,
  "resolved_at": null,
  "suppressed_until": null
}
```

## REST API Endpoints

### List Alerts

**Request**:
```
GET /api/v1/alerts?skip=0&limit=20&severity=critical&status=triggered
```

**Query Parameters**:
- `skip` (int, default=0): Records to skip for pagination
- `limit` (int, default=20, max=100): Records to return
- `inventory_id` (UUID, optional): Filter by inventory
- `anomaly_id` (UUID, optional): Filter by anomaly
- `severity` (string, optional): Filter by severity (high, critical)
- `status` (string, optional): Filter by status (triggered, acknowledged, resolved, suppressed)

**Response** (200 OK):
```json
{
  "total": 42,
  "count": 20,
  "skip": 0,
  "limit": 20,
  "items": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "anomaly_id": "660e8400-e29b-41d4-a716-446655440001",
      "inventory_id": "770e8400-e29b-41d4-a716-446655440002",
      "event_id": "evt-001",
      "severity": "critical",
      "status": "triggered",
      "title": "Alert: negative_inventory",
      "description": "Negative inventory detected",
      "triggered_at": "2026-07-24T10:30:45.123456+00:00",
      "acknowledged_at": null,
      "acknowledged_by": null,
      "resolved_at": null,
      "suppressed_until": null
    }
  ]
}
```

### Get Alert

**Request**:
```
GET /api/v1/alerts/{alert_id}
```

**Response** (200 OK):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "anomaly_id": "660e8400-e29b-41d4-a716-446655440001",
  "inventory_id": "770e8400-e29b-41d4-a716-446655440002",
  "event_id": "evt-001",
  "severity": "critical",
  "status": "triggered",
  "title": "Alert: negative_inventory",
  "description": "Negative inventory detected",
  "triggered_at": "2026-07-24T10:30:45.123456+00:00",
  "acknowledged_at": null,
  "acknowledged_by": null,
  "resolved_at": null,
  "suppressed_until": null
}
```

**Error** (404 Not Found):
```json
{
  "detail": "Alert not found"
}
```

### Acknowledge Alert

**Request**:
```
POST /api/v1/alerts/{alert_id}/acknowledge
Content-Type: application/json

{
  "acknowledged_by": "operator@company.com"
}
```

**Response** (200 OK):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "anomaly_id": "660e8400-e29b-41d4-a716-446655440001",
  "inventory_id": "770e8400-e29b-41d4-a716-446655440002",
  "event_id": "evt-001",
  "severity": "critical",
  "status": "acknowledged",
  "title": "Alert: negative_inventory",
  "description": "Negative inventory detected",
  "triggered_at": "2026-07-24T10:30:45.123456+00:00",
  "acknowledged_at": "2026-07-24T10:35:12.654321+00:00",
  "acknowledged_by": "operator@company.com",
  "resolved_at": null,
  "suppressed_until": null
}
```

### Resolve Alert

**Request**:
```
POST /api/v1/alerts/{alert_id}/resolve
Content-Type: application/json

{
  "resolved_by": "operator@company.com"
}
```

**Response** (200 OK):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  ...
  "status": "resolved",
  "resolved_at": "2026-07-24T10:40:30.123456+00:00",
  ...
}
```

### Suppress Alert

**Request**:
```
POST /api/v1/alerts/{alert_id}/suppress
Content-Type: application/json

{
  "suppressed_until": "2026-07-24T14:30:45.123456+00:00",
  "reason": "Issue under investigation, scheduled maintenance"
}
```

**Response** (200 OK):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  ...
  "status": "suppressed",
  "suppressed_until": "2026-07-24T14:30:45.123456+00:00",
  ...
}
```

### Get Alert Statistics

**Request**:
```
GET /api/v1/alerts/stats/summary
```

**Response** (200 OK):
```json
{
  "total_alerts": 152,
  "triggered_count": 18,
  "acknowledged_count": 42,
  "resolved_count": 87,
  "suppressed_count": 5,
  "critical_count": 32,
  "high_count": 120,
  "avg_ack_time_seconds": 245.5,
  "avg_resolution_time_seconds": 1850.25
}
```

## Usage Examples

### curl Examples

**List CRITICAL alerts**:
```bash
curl -X GET "http://localhost:8000/api/v1/alerts?severity=critical" \
  -H "Content-Type: application/json"
```

**Get specific alert**:
```bash
curl -X GET "http://localhost:8000/api/v1/alerts/550e8400-e29b-41d4-a716-446655440000" \
  -H "Content-Type: application/json"
```

**Acknowledge alert**:
```bash
curl -X POST "http://localhost:8000/api/v1/alerts/550e8400-e29b-41d4-a716-446655440000/acknowledge" \
  -H "Content-Type: application/json" \
  -d '{"acknowledged_by": "john.doe@example.com"}'
```

**Resolve alert**:
```bash
curl -X POST "http://localhost:8000/api/v1/alerts/550e8400-e29b-41d4-a716-446655440000/resolve" \
  -H "Content-Type: application/json" \
  -d '{"resolved_by": "john.doe@example.com"}'
```

**Suppress alert**:
```bash
curl -X POST "http://localhost:8000/api/v1/alerts/550e8400-e29b-41d4-a716-446655440000/suppress" \
  -H "Content-Type: application/json" \
  -d '{"suppressed_until": "2026-07-24T14:30:45Z"}'
```

**Get statistics**:
```bash
curl -X GET "http://localhost:8000/api/v1/alerts/stats/summary" \
  -H "Content-Type: application/json"
```

### Python Client Example

```python
import requests
from datetime import datetime, timedelta, timezone

BASE_URL = "http://localhost:8000/api/v1"

# List CRITICAL triggered alerts
response = requests.get(
    f"{BASE_URL}/alerts",
    params={
        "severity": "critical",
        "status": "triggered",
        "limit": 50
    }
)
alerts = response.json()

# Acknowledge first alert
if alerts["items"]:
    alert_id = alerts["items"][0]["id"]
    response = requests.post(
        f"{BASE_URL}/alerts/{alert_id}/acknowledge",
        json={"acknowledged_by": "ops@company.com"}
    )
    updated_alert = response.json()
    print(f"Alert {alert_id} acknowledged at {updated_alert['acknowledged_at']}")

# Get statistics
response = requests.get(f"{BASE_URL}/alerts/stats/summary")
stats = response.json()
print(f"Total alerts: {stats['total_alerts']}")
print(f"Critical alerts: {stats['critical_count']}")
print(f"Avg resolution time: {stats['avg_resolution_time_seconds']} seconds")
```

## Structured Logging

All alert operations are logged in structured JSON format for easy parsing and monitoring:

```json
{
  "timestamp": "2026-07-24T10:30:45.123456+00:00",
  "level": "INFO",
  "logger": "app.services.alert_service",
  "message": "alert_created",
  "module": "alert_service",
  "function": "create_alert",
  "line": 210,
  "alert_id": "550e8400-e29b-41d4-a716-446655440000",
  "anomaly_id": "660e8400-e29b-41d4-a716-446655440001",
  "severity": "critical"
}
```

### Log Events

- `alert_created`: Alert successfully created
- `alert_deduplicated`: Alert merged with existing alert
- `alert_acknowledged`: Alert acknowledged by user
- `alert_resolved`: Alert marked as resolved
- `alert_suppressed`: Alert suppressed until specified time
- `alert_published`: Alert published to Kafka
- `alert_generation_failed`: Error generating alert (non-blocking)

## Error Handling

The system implements graceful error handling:

### Non-Blocking Failures

Alert generation failures do not block event processing. If an alert cannot be generated or published:
1. Error is logged with full context
2. Event processing continues normally
3. Exception is caught and reported but not propagated

### Transient vs Non-Transient Errors

- **Transient Errors**: Database connection issues, temporary Kafka unavailability
  - Return HTTP 503 Service Unavailable
  - Can be retried by client
  - Logged as warnings initially

- **Non-Transient Errors**: Invalid requests, not found errors
  - Return appropriate HTTP status (4xx)
  - Should not be retried
  - Logged for investigation

## Testing

### Unit Tests

Run alert service unit tests:
```bash
pytest tests/test_alert_service.py -v
```

Tests cover:
- Alert creation with deduplication
- Alert querying with filters and pagination
- Alert lifecycle (acknowledge, resolve, suppress)
- Statistics calculation
- Error handling

### Integration Tests

Run alert endpoint integration tests:
```bash
pytest tests/test_alerts_endpoint.py -v
```

Tests cover:
- All REST endpoints
- Request/response validation
- Error conditions
- Pagination
- Filtering

### Run All Tests

```bash
pytest tests/test_alert*.py -v --cov=app.services.alert_service --cov=app.api.v1.endpoints.alerts
```

## Performance Considerations

### Database Indexes

The alerts table has strategic indexes for common queries:
- `(anomaly_id, triggered_at)`: Alert lookup by anomaly and time
- `(status, severity)`: Filtering by status and severity
- `(inventory_id, triggered_at)`: Inventory-specific alerts
- `(event_id)`: Unique constraint for idempotency

### Deduplication Performance

Deduplication uses indexed queries:
```sql
SELECT * FROM alerts
WHERE anomaly_id = ? 
  AND status != 'resolved'
  AND triggered_at >= now() - interval '5 minutes'
LIMIT 1
```

### Pagination

Always use pagination in production to avoid large result sets:
```python
# Good
response = requests.get("/api/v1/alerts?skip=0&limit=100")

# Avoid
response = requests.get("/api/v1/alerts")  # No limit, could return thousands
```

### Kafka Publishing

- Alerts published asynchronously to Kafka
- Non-blocking: publishing failure doesn't fail event processing
- Configured with acks="all" for durability
- Automatic retries with exponential backoff

## Monitoring and Observability

### Key Metrics to Monitor

1. **Alert Volume**:
   - Total triggered alerts per minute
   - Alerts by severity (CRITICAL vs HIGH)
   - Deduplication rate (actual vs generated)

2. **Alert Response Times**:
   - Average time to acknowledge
   - Average time to resolve
   - Stale alerts (>1 hour unacknowledged)

3. **System Health**:
   - Alert generation success rate
   - Kafka publishing success rate
   - API endpoint latency
   - Database query performance

### Sample Monitoring Queries

```python
# Get CRITICAL unresolved alerts
response = requests.get(
    "http://localhost:8000/api/v1/alerts",
    params={"severity": "critical", "status": "triggered"}
)
critical_alerts = response.json()["total"]

# Get statistics
response = requests.get("http://localhost:8000/api/v1/alerts/stats/summary")
stats = response.json()

# Alert resolution SLA
if stats["avg_resolution_time_seconds"]:
    sla_status = "🟢" if stats["avg_resolution_time_seconds"] < 1800 else "🔴"
    print(f"{sla_status} Avg resolution time: {stats['avg_resolution_time_seconds']}s (SLA: 30m)")
```

## Migration

To apply the alert system database migration:

```bash
# Upgrade to latest schema
alembic upgrade head

# To rollback (if needed)
alembic downgrade -1
```

## Future Enhancements

Potential improvements for future versions:

1. **Alert Rules Engine**: Custom alert rules based on conditions
2. **Alert Routing**: Route alerts to specific teams/channels
3. **Escalation Policies**: Automatic escalation for unresolved alerts
4. **Integration Hooks**: Webhooks, Slack, PagerDuty, email notifications
5. **Alert Correlation**: Group related alerts into incidents
6. **Machine Learning**: Anomaly detection to predict alert storms
7. **Advanced Analytics**: Alert trend analysis and root cause correlation
8. **Alert Templates**: Pre-configured alert messages by anomaly type

## Troubleshooting

### Alerts Not Generated

**Problem**: Anomalies detected but no alerts created

**Solutions**:
1. Check settings:
   ```bash
   # Verify alert_*_enabled settings
   curl http://localhost:8000/api/v1/alerts/stats/summary
   ```

2. Check logs for errors:
   ```bash
   docker logs inventory-sync-service | grep "alert_generation_failed"
   ```

3. Verify Kafka connectivity:
   ```bash
   docker logs inventory-sync-service | grep "kafka.*alert"
   ```

### High Deduplication Rate

**Problem**: Too many alerts being deduplicated

**Solutions**:
1. Increase deduplication window:
   ```bash
   ALERT_DEDUPLICATION_WINDOW_SECONDS=600  # 10 minutes instead of 5
   ```

2. Adjust anomaly detection thresholds to reduce noise
3. Analyze alert patterns to identify root causes

### Kafka Publishing Failures

**Problem**: Alerts created but not published to Kafka

**Solutions**:
1. Verify Kafka broker connectivity:
   ```bash
   nc -zv kafka-broker:9092
   ```

2. Check Kafka topic exists:
   ```bash
   kafka-topics --list --bootstrap-server kafka-broker:9092 | grep inventory_alerts
   ```

3. Verify producer configuration in settings
4. Check logs for detailed error messages

## Support and Contributions

For issues, questions, or contributions, please open an issue or pull request in the repository.
