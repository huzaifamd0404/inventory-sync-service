from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    error_code: str = Field(
        examples=[
            "inventory_event_publish_unavailable",
            "validation_error",
            "internal_server_error",
        ]
    )
    message: str = Field(examples=["Unable to publish inventory event"])
    request_id: str = Field(examples=["4c51bbf0c89a423a8346848f7be08ba8"])
    details: dict[str, object] | None = Field(default=None)


class RootResponse(BaseModel):
    service: str = Field(examples=["Inventory Sync Service"])
    version: str = Field(examples=["0.1.0"])
    status: str = Field(examples=["running"])
