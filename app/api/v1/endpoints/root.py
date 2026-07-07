from fastapi import APIRouter

from app.config.settings import get_settings

router = APIRouter()


@router.get("/")
async def root() -> dict[str, str]:
    settings = get_settings()
    return {
        "service": settings.app_name,
        "version": settings.app_version,
        "status": "running",
    }
