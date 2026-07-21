from fastapi import APIRouter, Depends, Request, Response, status

from app.schemas.health import HealthResponse
from app.services.health_service import HealthService

router = APIRouter()


def get_health_service() -> HealthService:
    return HealthService()


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Dependency health",
    description="Checks API, PostgreSQL, and optionally Redis connectivity.",
)
async def health(service: HealthService = Depends(get_health_service)) -> HealthResponse:
    return await service.check()


@router.get(
    "/health/live",
    response_model=HealthResponse,
    summary="Liveness probe",
    description="Kubernetes-style liveness probe indicating the API process is alive.",
)
async def health_live(service: HealthService = Depends(get_health_service)) -> HealthResponse:
    return await service.check_liveness()


@router.get(
    "/health/ready",
    response_model=HealthResponse,
    summary="Readiness probe",
    description="Readiness probe indicating whether dependencies are available for traffic.",
)
async def health_ready(
    request: Request,
    response: Response,
    service: HealthService = Depends(get_health_service),
) -> HealthResponse:
    health_response = await service.check_readiness()

    if getattr(request.app.state, "is_shutting_down", False):
        health_response.details["shutdown"] = "in_progress"
        health_response.status = "degraded"

    if health_response.status != "ok":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return health_response
