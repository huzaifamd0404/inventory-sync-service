from fastapi import APIRouter

from app.config.settings import get_settings
from app.schemas.common import RootResponse

router = APIRouter()


@router.get(
    "/",
    response_model=RootResponse,
    summary="Service status",
    description="Lightweight endpoint to verify API process availability.",
)
async def root() -> RootResponse:
    settings = get_settings()
    return RootResponse(service=settings.app_name, version=settings.app_version, status="running")
