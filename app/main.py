import logging
from contextlib import asynccontextmanager
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, Request

from app.api.v1.error_handlers import register_exception_handlers
from app.api.v1.router import api_router
from app.config.logging import configure_logging, set_request_id
from app.config.settings import get_settings

settings = get_settings()
configure_logging(settings.log_level)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Reserved for startup/shutdown resources (consumers, background workers).
    yield


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "Inventory Sync Service for publishing and processing inventory events through Kafka, "
        "persisting state in PostgreSQL, and synchronizing hot reads into Redis."
    ),
    docs_url="/docs",
    redoc_url=None,
    openapi_url="/openapi.json",
    lifespan=lifespan,
)


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id", uuid4().hex)
    request.state.request_id = request_id
    set_request_id(request_id)

    started = perf_counter()
    try:
        response = await call_next(request)
        elapsed_ms = round((perf_counter() - started) * 1000, 2)

        response.headers["x-request-id"] = request_id
        logger.info(
            "request_completed",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": elapsed_ms,
            },
        )
        return response
    finally:
        set_request_id(None)


register_exception_handlers(app)
app.include_router(api_router)
