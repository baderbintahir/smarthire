import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from smarthire_common.logging import request_id_var

logger = logging.getLogger(__name__)


class AppError(Exception):
    """Base for all expected, business-level errors. Raise it (or a subclass) anywhere;
    the handler below turns it into a consistent JSON response."""

    http_status: int = 400
    code: str = "BAD_REQUEST"  # stable, machine-readable; clients switch on this

    def __init__(self, message: str, *, code: str | None = None, http_status: int | None = None):
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        if http_status is not None:
            self.http_status = http_status


class NotFoundError(AppError):
    http_status = 404
    code = "NOT_FOUND"


class ConflictError(AppError):
    http_status = 409
    code = "CONFLICT"


def _error_response(status: int, code: str, message: str, details=None) -> JSONResponse:
    body = {"code": code, "message": message, "request_id": request_id_var.get()}
    if details is not None:
        body["details"] = details
    return JSONResponse(status_code=status, content={"error": body})


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def handle_app_error(request: Request, exc: AppError):
        return _error_response(exc.http_status, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(request: Request, exc: RequestValidationError):
        # Same body shape as every other error; field-level problems go in "details"
        return _error_response(
            422, "VALIDATION_ERROR", "Request validation failed", jsonable_details(exc)
        )

    @app.exception_handler(Exception)
    async def handle_unexpected(request: Request, exc: Exception):
        # Full traceback goes to the logs; the client only gets a generic message + request_id
        logger.exception("Unhandled exception")
        return _error_response(500, "INTERNAL_ERROR", "Internal server error")


def jsonable_details(exc: RequestValidationError) -> list[dict]:
    # Keep only safe, useful keys (location + message); drop the raw input value
    return [{"loc": list(e["loc"]), "msg": e["msg"]} for e in exc.errors()]
