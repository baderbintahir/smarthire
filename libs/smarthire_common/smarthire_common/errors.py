import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from smarthire_common.logging import request_id_var

logger = logging.getLogger(__name__)


class AppError(Exception):
    """Base for all expected, business-level errors. Raise it (or a subclass) anywhere;
    the handler below turns it into a consistent JSON response."""

    http_status: int = 400
    code: str = "BAD_REQUEST"  # stable, machine-readable; clients switch on this

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        http_status: int | None = None,
        headers: dict[str, str] | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.headers = headers or {}  # extra response headers, e.g. Retry-After
        if code is not None:
            self.code = code
        if http_status is not None:
            self.http_status = http_status


class UnauthorizedError(AppError):
    """401: missing, invalid, expired or revoked credentials ("who are you?")."""

    http_status = 401
    code = "UNAUTHORIZED"

    def __init__(self, message: str = "Authentication required", **kwargs):
        # RFC 6750: tell the client which auth scheme is expected
        kwargs.setdefault("headers", {"WWW-Authenticate": "Bearer"})
        super().__init__(message, **kwargs)


class ForbiddenError(AppError):
    """403: valid identity, but not allowed to do this ("I know you, and no")."""

    http_status = 403
    code = "FORBIDDEN"


class NotFoundError(AppError):
    http_status = 404
    code = "NOT_FOUND"


class ConflictError(AppError):
    http_status = 409
    code = "CONFLICT"


class TooManyRequestsError(AppError):
    http_status = 429
    code = "TOO_MANY_REQUESTS"

    def __init__(self, message: str = "Too many requests", *, retry_after: int, **kwargs):
        kwargs.setdefault("headers", {"Retry-After": str(retry_after)})
        super().__init__(message, **kwargs)


class ServiceUnavailableError(AppError):
    """503: a dependency we need right now is down; retrying later may work."""

    http_status = 503
    code = "SERVICE_UNAVAILABLE"


def _error_response(
    status: int, code: str, message: str, details=None, headers: dict[str, str] | None = None
) -> JSONResponse:
    body = {"code": code, "message": message, "request_id": request_id_var.get()}
    if details is not None:
        body["details"] = details
    return JSONResponse(status_code=status, content={"error": body}, headers=headers)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def handle_app_error(request: Request, exc: AppError):
        return _error_response(exc.http_status, exc.code, exc.message, headers=exc.headers)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(request: Request, exc: RequestValidationError):
        # Same body shape as every other error; field-level problems go in "details"
        return _error_response(
            422, "VALIDATION_ERROR", "Request validation failed", jsonable_details(exc)
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_exception(request: Request, exc: StarletteHTTPException):
        # Errors raised by the framework itself (unknown route -> 404, wrong method -> 405)
        return _error_response(exc.status_code, f"HTTP_{exc.status_code}", str(exc.detail))

    @app.exception_handler(Exception)
    async def handle_unexpected(request: Request, exc: Exception):
        # Full traceback goes to the logs; the client only gets a generic message + request_id
        logger.exception("Unhandled exception")
        return _error_response(500, "INTERNAL_ERROR", "Internal server error")


def jsonable_details(exc: RequestValidationError) -> list[dict]:
    # Keep only safe, useful keys (location + message); drop the raw input value
    return [{"loc": list(e["loc"]), "msg": e["msg"]} for e in exc.errors()]
