from datetime import UTC, datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class InventoryItemSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sku: str = Field(min_length=1, max_length=128)
    quantity: int = Field(ge=0)
    warehouse_id: str = Field(min_length=1, max_length=128)


class InventoryOperation(str, Enum):
    SALE = "SALE"
    RESTOCK = "RESTOCK"
    RETURN = "RETURN"
    MANUAL_ADJUSTMENT = "MANUAL_ADJUSTMENT"


class InventoryEventCreate(BaseModel):
    product_id: str = Field(min_length=1, max_length=128)
    store_id: str = Field(min_length=1, max_length=128)
    operation: InventoryOperation
    quantity: int
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_quantity_for_operation(self) -> "InventoryEventCreate":
        if self.operation == InventoryOperation.MANUAL_ADJUSTMENT and self.quantity == 0:
            raise ValueError("quantity must be non-zero for MANUAL_ADJUSTMENT")

        if (
            self.operation in {InventoryOperation.SALE, InventoryOperation.RESTOCK, InventoryOperation.RETURN}
            and self.quantity <= 0
        ):
            raise ValueError(f"quantity must be positive for {self.operation.value}")

        return self

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
