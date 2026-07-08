from fastapi import APIRouter, Depends

from app.schemas.health import HealthResponse
from app.services.health_service import HealthService

router = APIRouter()


def get_health_service() -> HealthService:
    return HealthService()


@router.get("/health", response_model=HealthResponse)
async def health(service: HealthService = Depends(get_health_service)) -> HealthResponse:
    return await service.check()
