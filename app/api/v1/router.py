from fastapi import APIRouter

from app.api.v1.endpoints.anomalies import router as anomalies_router
from app.api.v1.endpoints.health import router as health_router
from app.api.v1.endpoints.inventory_events import router as inventory_events_router
from app.api.v1.endpoints.metrics import router as metrics_router
from app.api.v1.endpoints.observability import router as observability_router
from app.api.v1.endpoints.reconciliation import router as reconciliation_router
from app.api.v1.endpoints.root import router as root_router
from app.api.v1.endpoints.sales import router as sales_router

api_router = APIRouter()
api_router.include_router(root_router, tags=["root"])
api_router.include_router(health_router, tags=["health"])
api_router.include_router(inventory_events_router, tags=["inventory-events"])
api_router.include_router(sales_router, tags=["sales"])
api_router.include_router(anomalies_router, tags=["anomalies"])
api_router.include_router(observability_router, tags=["observability"])
api_router.include_router(metrics_router, tags=["metrics"])
api_router.include_router(reconciliation_router, tags=["reconciliation"])
