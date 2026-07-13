from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = Field(examples=["ok", "degraded"])
    service: str = Field(examples=["Inventory Sync Service"])
    version: str = Field(examples=["0.1.0"])
    details: dict[str, str] = Field(examples=[{"api": "ok", "postgres": "ok", "redis": "ok"}])
