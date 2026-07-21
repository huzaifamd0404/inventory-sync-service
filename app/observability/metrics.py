from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

INVENTORY_EVENTS_PROCESSED_TOTAL = Counter(
    "inventory_events_processed_total",
    "Total number of successfully processed inventory events.",
)

INVENTORY_EVENTS_FAILED_TOTAL = Counter(
    "inventory_events_failed_total",
    "Total number of inventory events that failed processing.",
)

INVENTORY_EVENTS_DUPLICATE_TOTAL = Counter(
    "inventory_events_duplicate_total",
    "Total number of duplicate inventory events skipped.",
)

INVENTORY_EVENTS_RETRIED_TOTAL = Counter(
    "inventory_events_retried_total",
    "Total number of retry attempts for inventory event processing.",
)

INVENTORY_EVENTS_DLQ_TOTAL = Counter(
    "inventory_events_dlq_total",
    "Total number of inventory events published to dead letter queue.",
)

INVENTORY_EVENT_PROCESSING_DURATION_SECONDS = Histogram(
    "inventory_event_processing_duration_seconds",
    "Duration of inventory event processing in seconds.",
    labelnames=("outcome",),
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)


def record_processed_events(count: int = 1) -> None:
    if count > 0:
        INVENTORY_EVENTS_PROCESSED_TOTAL.inc(count)


def record_failed_events(count: int = 1) -> None:
    if count > 0:
        INVENTORY_EVENTS_FAILED_TOTAL.inc(count)


def record_duplicate_events(count: int = 1) -> None:
    if count > 0:
        INVENTORY_EVENTS_DUPLICATE_TOTAL.inc(count)


def record_retried_events(count: int = 1) -> None:
    if count > 0:
        INVENTORY_EVENTS_RETRIED_TOTAL.inc(count)


def record_dlq_events(count: int = 1) -> None:
    if count > 0:
        INVENTORY_EVENTS_DLQ_TOTAL.inc(count)


def observe_processing_duration(seconds: float, outcome: str) -> None:
    if seconds >= 0:
        INVENTORY_EVENT_PROCESSING_DURATION_SECONDS.labels(outcome=outcome).observe(seconds)


def generate_prometheus_metrics() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
