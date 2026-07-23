# Anomaly Detection Engine - Implementation Guide

## Overview

The Inventory Sync Service now includes a production-ready, rule-based anomaly detection engine that monitors inventory and sales events for suspicious patterns and anomalies. The engine runs automatically during event processing and exposes REST APIs for querying detected anomalies.

## Architecture

### Core Components

#### 1. **AnomalyDetectionService** (`app/services/anomaly_detection_service.py`)

The main anomaly detection engine implementing a rule-based architecture:

- **Purpose**: Evaluates inventory state against configurable business rules
- **Features**:
  - Pluggable rule system for extensibility
  - Default rule set included
  - Support for custom rule implementations
  - Atomic anomaly persistence
  - Comprehensive structured logging

**Key Methods**:
- `detect_anomalies(inventory_id, event_id)`: Evaluate all rules for an inventory item
- `persist_anomalies(anomalies)`: Persist detected anomalies to database
- `add_rule(rule)`: Register custom detection rule
- `remove_rule(rule_type)`: Unregister detection rule

#### 2. **AnomalyService** (`app/services/anomaly_service.py`)

Query and mutation service for anomalies:

- **Purpose**: Manage anomaly lifecycle and provide query interfaces
- **Features**:
  - List anomalies with pagination
  - Filter by inventory, type, severity, status
  - Retrieve single anomalies
  - Update anomaly status
  - Generate statistics
  - Error handling and transient retry support

**Key Methods**:
- `list_anomalies(...)`: Paginated anomaly listing with filters
- `get_anomaly(anomaly_id)`: Retrieve specific anomaly
- `get_anomalies_by_inventory(inventory_id, status)`: Get all anomalies for inventory
- `update_anomaly_status(anomaly_id, status)`: Update anomaly status
- `get_stats()`: Get summary statistics

#### 3. **REST API** (`app/api/v1/endpoints/anomalies.py`)

FastAPI endpoints for anomaly management:

- **Endpoints**:
  - `GET /api/v1/anomalies` - List anomalies with filters and pagination
  - `GET /api/v1/anomalies/{anomaly_id}` - Get specific anomaly
  - `GET /api/v1/anomalies/inventory/{inventory_id}` - Get anomalies for inventory
  - `PATCH /api/v1/anomalies/{anomaly_id}/status` - Update anomaly status
  - `GET /api/v1/anomalies/stats/summary` - Get statistics

#### 4. **Database Model** (`app/database/models.py`)

Extended Anomaly model with comprehensive fields:

```python
class Anomaly(Base):
    id: UUID                          # Unique anomaly identifier
    inventory_id: UUID                # Associated inventory item
    event_id: str                     # Source event (optional, unique)
    anomaly_type: str                 # Type of anomaly detected
    severity: AnomalySeverity         # CRITICAL, HIGH, MEDIUM, LOW
    score: float                      # Confidence score (0-100)
    status: AnomalyStatus             # OPEN, INVESTIGATING, RESOLVED
    description: str                  # Detailed description
    detected_at: datetime             # When anomaly was detected
    resolved_at: datetime             # When anomaly was resolved (optional)
```

## Detection Rules

### 1. **NegativeInventoryRule**

Detects inventory quantities below zero.

**Configuration**: None (always active)

**Severity**: CRITICAL

**Use Case**: Database constraint violation or synchronization error

```python
# Example detection
inventory.quantity = -5  # Triggers anomaly
```

### 2. **SuddenSalesSpike**

Detects unusually large sales transactions compared to historical average.

**Configuration**:
- `spike_multiplier` (default: 5.0) - Multiple of average to trigger
- `lookback_hours` (default: 24) - Hours of history to analyze
- `min_prior_sales` (default: 5) - Minimum sales for baseline

**Severity Calculation**:
- `> 10x average`: CRITICAL
- `> 7x average`: HIGH
- `> 5x average`: MEDIUM
- Otherwise: LOW

**Use Case**: Detect unusual demand patterns, potentially fraudulent activity

```python
# Example: Historical average is 2 units
# New sale: 15 units (7.5x average) -> HIGH severity anomaly
```

### 3. **LargeInventoryAdjustment**

Detects unusually large inventory adjustments.

**Configuration**:
- `adjustment_threshold_percent` (default: 50.0) - Percentage of stock to trigger
- `lookback_hours` (default: 24) - Hours to look back

**Severity Calculation**:
- `> 150% adjustment`: CRITICAL
- `> 100% adjustment`: HIGH
- `> 75% adjustment`: MEDIUM
- Otherwise: LOW

**Use Case**: Detect data entry errors, inventory reconciliation issues

```python
# Example: Inventory before: 100, after: 30 (70% adjustment) -> HIGH severity
```

### 4. **RapidConsecutiveSales**

Detects rapid, consecutive sales transactions.

**Configuration**:
- `transaction_count` (default: 5) - Number of sales to trigger
- `time_window_seconds` (default: 60) - Time window for rapid sales

**Severity Calculation**:
- `> 15 transactions`: CRITICAL
- `> 10 transactions`: HIGH
- `> 7 transactions`: MEDIUM
- Otherwise: LOW

**Use Case**: Detect potential automated/bot activity, system issues

```python
# Example: 6 sales in 60 seconds -> HIGH severity anomaly
```

## Integration with Event Processing Pipeline

Anomaly detection integrates seamlessly into the batch event processing pipeline:

### Flow

```
Kafka Event -> Batch Consumer -> Batch Processing Service
                                           |
                                           v
                                   Inventory Service
                                   (process event)
                                           |
                                           v
                                Anomaly Detection Service
                                (detect & persist)
                                           |
                                           v
                                   Database Storage
```

### Implementation Details

1. **After successful inventory event processing**, the batch processor triggers anomaly detection
2. **Anomaly detection** retrieves affected inventory and evaluates all rules
3. **Detected anomalies** are persisted atomically with idempotency check (event_id based)
4. **Failures** in anomaly detection don't fail the main event processing (graceful degradation)
5. **Structured logging** captures all anomaly detection activities

## API Usage Examples

### List Anomalies with Filtering

```bash
# Get all open anomalies
curl "http://localhost:8000/api/v1/anomalies?status=open"

# Get critical severity anomalies
curl "http://localhost:8000/api/v1/anomalies?severity=critical"

# Paginated query with filters
curl "http://localhost:8000/api/v1/anomalies?status=open&severity=high&skip=0&limit=20"

# By anomaly type
curl "http://localhost:8000/api/v1/anomalies?anomaly_type=negative_inventory"
```

### Get Specific Anomaly

```bash
curl "http://localhost:8000/api/v1/anomalies/550e8400-e29b-41d4-a716-446655440000"
```

### Get Anomalies for Inventory Item

```bash
curl "http://localhost:8000/api/v1/anomalies/inventory/550e8400-e29b-41d4-a716-446655440001"

# With status filter
curl "http://localhost:8000/api/v1/anomalies/inventory/550e8400-e29b-41d4-a716-446655440001?status=open"
```

### Update Anomaly Status

```bash
# Mark as investigating
curl -X PATCH \
  "http://localhost:8000/api/v1/anomalies/550e8400-e29b-41d4-a716-446655440000/status" \
  -H "Content-Type: application/json" \
  -d '{"status": "investigating"}'

# Mark as resolved
curl -X PATCH \
  "http://localhost:8000/api/v1/anomalies/550e8400-e29b-41d4-a716-446655440000/status" \
  -H "Content-Type: application/json" \
  -d '{"status": "resolved"}'
```

### Get Statistics

```bash
curl "http://localhost:8000/api/v1/anomalies/stats/summary"

# Response:
{
  "total_anomalies": 42,
  "open_anomalies": 15,
  "investigating_anomalies": 8,
  "resolved_anomalies": 19,
  "critical_count": 3,
  "high_count": 12,
  "medium_count": 15,
  "low_count": 12,
  "anomaly_types_count": {
    "negative_inventory": 8,
    "sudden_sales_spike": 20,
    "large_inventory_adjustment": 10,
    "rapid_consecutive_sales": 4
  }
}
```

## Configuration

### Database Migrations

A new migration is required to add the `event_id` field:

```bash
alembic revision --autogenerate -m "add_event_id_to_anomalies"
alembic upgrade head
```

### Rule Configuration

Customize detection rules via environment variables or configuration:

```python
from app.services.anomaly_detection_service import (
    AnomalyDetectionService,
    SuddenSalesSpike,
    LargeInventoryAdjustment,
)

# Custom rule setup
service = AnomalyDetectionService(
    session_factory=SessionLocal,
    rules=[
        NegativeInventoryRule(),
        SuddenSalesSpike(spike_multiplier=3.0, lookback_hours=12),
        LargeInventoryAdjustment(adjustment_threshold_percent=30.0),
        RapidConsecutiveSales(transaction_count=10),
    ]
)
```

## Structured Logging

All anomaly detection operations are logged with structured fields for observability:

```python
# Anomaly detected
{
  "timestamp": "2026-07-23T10:15:30Z",
  "level": "INFO",
  "message": "anomaly_detected",
  "anomaly_type": "negative_inventory",
  "severity": "critical",
  "score": 100.0,
  "inventory_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_id": "EVT-001"
}

# Anomaly persisted
{
  "timestamp": "2026-07-23T10:15:31Z",
  "level": "INFO",
  "message": "anomalies_persisted",
  "count": 2
}
```

## Error Handling

The system implements production-ready error handling:

### Transient Errors
- Database connection issues
- Temporary service unavailability
- Automatic retry with exponential backoff

### Non-Retryable Errors
- Inventory not found
- Invalid parameters
- Data constraint violations

### Graceful Degradation
- If anomaly detection fails, event processing continues
- Anomalies are logged but don't block the pipeline
- Missing anomaly data doesn't cause data loss

## Testing

### Unit Tests

Located in `tests/test_anomaly_detection_service.py`:

```bash
pytest tests/test_anomaly_detection_service.py -v
```

Tests cover:
- Each detection rule individually
- Multiple simultaneous anomalies
- Idempotent persistence
- Custom rule addition/removal

### Service Tests

Located in `tests/test_anomaly_service.py`:

```bash
pytest tests/test_anomaly_service.py -v
```

Tests cover:
- Pagination and filtering
- Query operations
- Status updates
- Statistics generation

### Integration Tests

Located in `tests/test_anomalies_endpoint.py`:

```bash
pytest tests/test_anomalies_endpoint.py -v
```

Tests cover:
- All REST API endpoints
- Error handling
- Response formats
- Status codes

### Run All Tests

```bash
pytest tests/test_anomaly*.py -v --cov=app/services/anomaly* --cov=app/api/v1/endpoints/anomalies
```

## Performance Considerations

### Database Indexes

The Anomaly model includes optimized indexes:

```python
# Fast queries by inventory and detection time
Index("ix_anomalies_inventory_detected_at", "inventory_id", "detected_at")

# Fast status/severity filtering
Index("ix_anomalies_status_severity", "status", "severity")

# Unique constraint on event_id for idempotency
Index("ix_anomalies_event_id", "event_id", unique=True)
```

### Query Optimization

- Pagination limited to 100 items max
- Offset-based pagination for stability
- Proper index utilization in filters
- Connection pooling for database

### Detection Performance

- Rule evaluation is O(N) where N = historical records
- Batch processing reduces database round trips
- Anomaly detection runs asynchronously (non-blocking)

## Extending the System

### Adding Custom Rules

```python
from app.services.anomaly_detection_service import AnomalyDetectionRule, AnomalyDetectionResult

class CustomRule(AnomalyDetectionRule):
    def evaluate(self, inventory, session):
        # Custom logic here
        if condition:
            return AnomalyDetectionResult(
                detected=True,
                anomaly_type="custom_anomaly",
                severity=AnomalySeverity.HIGH,
                score=80.0,
                description="Custom anomaly description"
            )
        return AnomalyDetectionResult(detected=False)

# Register rule
service.add_rule(CustomRule())
```

### Monitoring and Alerting

Integrate with observability stack:

```python
# Prometheus metrics example
from prometheus_client import Counter

anomalies_detected = Counter(
    'anomalies_detected_total',
    'Total anomalies detected',
    ['anomaly_type', 'severity']
)

# In anomaly detection
anomalies_detected.labels(
    anomaly_type='negative_inventory',
    severity='critical'
).inc()
```

## Troubleshooting

### Anomalies Not Detected

1. **Check anomaly detection is enabled** in batch processor configuration
2. **Verify rules are active** - check logs for rule evaluation
3. **Check database connectivity** - connection pool issues
4. **Verify rule conditions** - may need different thresholds

### Performance Degradation

1. **Check historical data size** - large history lookback impacts performance
2. **Verify indexes exist** - run database optimization
3. **Check concurrent anomaly detection** - batch size affects throughput

### Anomalies Not Persisting

1. **Check database permissions** - write access required
2. **Verify unique constraint** - check for duplicate event_ids
3. **Check transaction isolation** - serialization conflicts

## Future Enhancements

Potential improvements to consider:

1. **Machine Learning Rules** - Integrate ML-based anomaly detection
2. **Rule Scheduling** - Run detection on schedule, not just on events
3. **Anomaly Grouping** - Correlate related anomalies
4. **Alert Routing** - Integrate with alerting systems
5. **Custom Thresholds** - Per-inventory-item rule customization
6. **Anomaly Remediation** - Automated actions for certain anomalies
7. **Feedback Loop** - Learn from human resolutions

## References

- **Architecture**: Clean Architecture, SOLID Principles
- **Framework**: FastAPI 0.100+, SQLAlchemy 2.0+
- **Validation**: Pydantic v2
- **Database**: PostgreSQL 13+
- **Testing**: pytest, pytest-cov
