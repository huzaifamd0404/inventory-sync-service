from fastapi import APIRouter, Response

from app.observability.metrics import generate_prometheus_metrics

router = APIRouter(tags=["observability"])


@router.get(
    "/metrics",
    summary="Prometheus metrics",
    description="Prometheus scrape endpoint for runtime and business metrics.",
)
async def prometheus_metrics() -> Response:
    metrics_payload, content_type = generate_prometheus_metrics()
    return Response(content=metrics_payload, media_type=content_type)
