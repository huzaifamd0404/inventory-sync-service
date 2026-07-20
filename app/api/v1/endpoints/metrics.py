"""Metrics endpoint for monitoring batch processing performance."""
import logging

from fastapi import APIRouter, Depends

from app.schemas.metrics import BatchMetricsResponse
from app.services.batch_processing_service import BatchProcessingService, get_batch_processing_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/metrics", tags=["metrics"])


def get_batch_service() -> BatchProcessingService:
    """Dependency injection for batch processing service."""
    factory = get_batch_processing_service()
    return factory()


@router.get(
    "",
    response_model=BatchMetricsResponse,
    summary="Get batch processing metrics",
    description=(
        "Retrieve comprehensive metrics about batch event processing performance, "
        "including success rates, processing times, error counts, and cache/database operations."
    ),
)
async def get_metrics(
    batch_service: BatchProcessingService = Depends(get_batch_service),
) -> BatchMetricsResponse:
    """
    Get current batch processing metrics.

    Returns comprehensive metrics including:
    - Batch processing statistics (total processed, success/failure counts)
    - Performance metrics (min/max/average processing times)
    - Database operation metrics
    - Redis pipeline metrics
    - Last update timestamp

    Returns:
        BatchMetricsResponse: Current metrics snapshot
    """
    logger.debug(
        "metrics_endpoint_called",
    )

    metrics_dict = batch_service.metrics.to_dict()
    return BatchMetricsResponse(**metrics_dict)
