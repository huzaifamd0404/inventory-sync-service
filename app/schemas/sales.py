from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SalesEventCreate(BaseModel):
    sale_id: str = Field(
        min_length=1,
        max_length=128,
        description="Client-supplied idempotency key for this sale transaction.",
        examples=["ORDER-20260722-001"],
    )
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
    quantity_sold: int = Field(
        ge=1,
        le=1_000_000,
        description="Number of units sold. Must be a positive integer.",
        examples=[5],
    )
    sale_price: Decimal | None = Field(
        default=None,
        description="Unit sale price. Optional.",
        examples=["29.99"],
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timezone-aware timestamp when the sale occurred.",
        examples=["2026-07-22T10:00:00Z"],
    )

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

    @field_validator("sale_id")
    @classmethod
    def validate_sale_id_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("sale_id must not be blank")
        return value

    @field_validator("sale_price")
    @classmethod
    def validate_sale_price_non_negative(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and value < Decimal("0"):
            raise ValueError("sale_price must be non-negative")
        return value


class SalesEvent(SalesEventCreate):
    event_id: UUID = Field(description="Server-generated unique event identifier.")


class SalesRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    inventory_id: UUID
    quantity_sold: int
    sale_price: Decimal | None
    external_sale_id: str | None
    sold_at: datetime


class SalesEventPublishResponse(BaseModel):
    event_id: UUID = Field(
        description="Accepted event identifier that can be traced in consumer logs.",
        examples=["0e9f4d70-98a3-41f3-b9bc-7439f4ac0f57"],
    )
    sale_id: str = Field(
        description="Idempotency key echoed back to the caller.",
        examples=["ORDER-20260722-001"],
    )


class SalesSummaryResponse(BaseModel):
    product_id: str = Field(examples=["SKU-100"])
    store_id: str = Field(examples=["STORE-NYC"])
    total_quantity_sold: int = Field(examples=[42])
    transaction_count: int = Field(examples=[10])
    total_revenue: Decimal | None = Field(
        default=None,
        description="Sum of (sale_price × quantity_sold) for all priced transactions.",
        examples=["1259.58"],
    )
    sales: list[SalesRecord]
