from __future__ import annotations


class InventoryProcessingException(Exception):
    """Base exception for inventory event processing failures."""


class InvalidInventoryEvent(InventoryProcessingException):
    """Raised when an inventory event payload fails business validation."""


class DuplicateEvent(InventoryProcessingException):
    """Raised when an event was already processed and should be skipped."""


class InventoryOperationError(InventoryProcessingException):
    """Raised when an inventory operation cannot be applied safely."""


class KafkaProcessingError(InventoryProcessingException):
    """Raised when Kafka processing infrastructure fails."""
