from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.database.models import ReconciliationStatus


class ReconciliationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)

    store_id: str = Field(
        examples=["STORE-NYC"],
        description="Store or warehouse identifier.",
    )
    product_id: str = Field(
        examples=["SKU-100"],
        description="Product SKU or canonical inventory identifier.",
    )
    expected_quantity: int = Field(
        description="Quantity derived by summing all recorded inventory history deltas.",
        examples=[42],
    )
    actual_quantity: int = Field(
        description="Current quantity held in the inventory table.",
        examples=[40],
    )
    difference: int = Field(
        description="actual_quantity minus expected_quantity. Non-zero indicates drift.",
        examples=[-2],
    )
    status: ReconciliationStatus = Field(
        description=(
            "'match' when actual equals expected; "
            "'mismatch' when they diverge; "
            "'missing' when no inventory record exists for the given store/product pair."
        ),
        examples=["mismatch"],
    )
    reconciled_at: datetime = Field(
        description="UTC timestamp of the most recent reconciliation that changed state.",
        examples=["2026-07-17T10:00:00Z"],
    )
