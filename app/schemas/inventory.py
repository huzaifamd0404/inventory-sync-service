from pydantic import BaseModel, ConfigDict, Field


class InventoryItemSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sku: str = Field(min_length=1, max_length=128)
    quantity: int = Field(ge=0)
    warehouse_id: str = Field(min_length=1, max_length=128)
