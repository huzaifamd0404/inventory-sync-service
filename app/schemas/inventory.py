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
    product_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_\-:.]*$",
        description="Product SKU or canonical inventory identifier.",
        examples=["SKU-100"],
    )
    store_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_\-:.]*$",
        description="Store or warehouse identifier.",
        examples=["STORE-NYC"],
    )
    operation: InventoryOperation
    quantity: int = Field(
        ge=-1_000_000,
        le=1_000_000,
        description=(
            "Operation quantity. SALE/RESTOCK/RETURN require positive values; "
            "MANUAL_ADJUSTMENT supports positive or negative non-zero values."
        ),
        examples=[5],
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timezone-aware timestamp when the event was created.",
        examples=["2026-07-09T11:00:00Z"],
    )

    @model_validator(mode="after")
    def validate_quantity_for_operation(self) -> "InventoryEventCreate":
        if self.operation == InventoryOperation.MANUAL_ADJUSTMENT and self.quantity == 0:
            raise ValueError("quantity must be non-zero for MANUAL_ADJUSTMENT")

        if (
            self.operation
            in {InventoryOperation.SALE, InventoryOperation.RESTOCK, InventoryOperation.RETURN}
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

    @field_validator("product_id", "store_id")
    @classmethod
    def validate_identifier_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("identifier must not be blank")
        return value


class InventoryEvent(InventoryEventCreate):
    event_id: UUID = Field(description="Server-generated unique event identifier")


class InventoryEventPublishResponse(BaseModel):
    event_id: UUID = Field(
        description="Accepted event identifier that can be traced in consumer logs",
        examples=["0e9f4d70-98a3-41f3-b9bc-7439f4ac0f57"],
    )
