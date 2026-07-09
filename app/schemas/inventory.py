from datetime import UTC, datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class InventoryItemSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sku: str = Field(min_length=1, max_length=128)
    quantity: int = Field(ge=0)
    warehouse_id: str = Field(min_length=1, max_length=128)


class InventoryOperation(str, Enum):
    create = "create"
    update = "update"
    delete = "delete"
    adjust = "adjust"


class InventoryEventCreate(BaseModel):
    product_id: str = Field(min_length=1, max_length=128)
    store_id: str = Field(min_length=1, max_length=128)
    operation: InventoryOperation
    quantity: int = Field(ge=0)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp_is_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value


class InventoryEvent(InventoryEventCreate):
    event_id: UUID


class InventoryEventPublishResponse(BaseModel):
    event_id: UUID
