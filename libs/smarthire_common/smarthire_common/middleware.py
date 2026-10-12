import uuid

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from smarthire_common.logging import request_id_var

REQUEST_ID_HEADER = "X-Request-ID"
MAX_REQUEST_ID_LENGTH = 128


class RequestIdMiddleware:
    """Gives every HTTP request an id: reuses the caller's X-Request-ID (e.g. from the gateway)
    or generates one, stores it in request_id_var so every log line carries it, and echoes it
    in the response header.

    Written as plain ASGI (not BaseHTTPMiddleware) so the ContextVar reliably reaches the
    endpoint code. It is deliberately NOT reset at the end: the server runs each request in
    its own task, and the outer 500 handler still needs the id after we are unwound."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = Headers(scope=scope).get(REQUEST_ID_HEADER, "")
        request_id = incoming[:MAX_REQUEST_ID_LENGTH] or str(uuid.uuid4())
        request_id_var.set(request_id)

        async def send_with_request_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)[REQUEST_ID_HEADER] = request_id
            await send(message)

        await self.app(scope, receive, send_with_request_id)
