from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.schemas.common import ErrorResponse

logger = logging.getLogger(__name__)


def _build_error_response(
    *,
    status_code: int,
    error_code: str,
    message: str,
    request_id: str,
    details: dict[str, object] | None = None,
) -> JSONResponse:
    payload = ErrorResponse(
        error_code=error_code,
        message=message,
        request_id=request_id,
        details=details,
    )
    return JSONResponse(status_code=status_code, content=payload.model_dump())


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        request_id = getattr(request.state, "request_id", "unknown")
        detail = exc.detail if isinstance(exc.detail, str) else "request failed"
        logger.warning(
            "http_exception",
            extra={
                "request_id": request_id,
                "status_code": exc.status_code,
                "path": request.url.path,
                "method": request.method,
                "detail": detail,
            },
        )
        return _build_error_response(
            status_code=exc.status_code,
            error_code="http_error",
            message=detail,
            request_id=request_id,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        request_id = getattr(request.state, "request_id", "unknown")
        logger.warning(
            "request_validation_failed",
            extra={
                "request_id": request_id,
                "path": request.url.path,
                "method": request.method,
                "errors": exc.errors(),
            },
        )
        return _build_error_response(
            status_code=422,
            error_code="validation_error",
            message="request validation failed",
            request_id=request_id,
            details={"errors": exc.errors()},
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        request_id = getattr(request.state, "request_id", "unknown")
        logger.exception(
            "unhandled_exception",
            extra={
                "request_id": request_id,
                "path": request.url.path,
                "method": request.method,
            },
        )
        return _build_error_response(
            status_code=500,
            error_code="internal_server_error",
            message="an unexpected server error occurred",
            request_id=request_id,
        )
