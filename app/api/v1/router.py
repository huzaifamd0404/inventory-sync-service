from fastapi import APIRouter

from app.api.v1.endpoints.health import router as health_router
from app.api.v1.endpoints.inventory_events import router as inventory_events_router
from app.api.v1.endpoints.root import router as root_router

api_router = APIRouter()
api_router.include_router(root_router, tags=["root"])
api_router.include_router(health_router, tags=["health"])
api_router.include_router(inventory_events_router, tags=["inventory-events"])
