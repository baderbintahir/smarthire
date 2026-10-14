import json
import logging

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from smarthire_common.config import BaseServiceSettings
from smarthire_common.errors import ConflictError, NotFoundError, register_exception_handlers
from smarthire_common.logging import JsonFormatter, request_id_var


class Body(BaseModel):
    email: str


def make_client() -> TestClient:
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/missing")
    async def missing():
        raise NotFoundError("Job not found")

    @app.get("/dup")
    async def dup():
        raise ConflictError("Email taken", code="EMAIL_TAKEN")

    @app.get("/boom")
    async def boom():
        raise RuntimeError("secret db password in message")

    @app.post("/body")
    async def body(b: Body):
        return b

    # raise_server_exceptions=False: let the 500 handler answer instead of re-raising in the test
    return TestClient(app, raise_server_exceptions=False)


def test_app_error_has_consistent_body():
    r = make_client().get("/missing")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "NOT_FOUND"
    assert r.json()["error"]["message"] == "Job not found"


def test_app_error_custom_code():
    r = make_client().get("/dup")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "EMAIL_TAKEN"


def test_validation_error_uses_same_shape():
    r = make_client().post("/body", json={})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "VALIDATION_ERROR"
    assert r.json()["error"]["details"][0]["loc"] == ["body", "email"]


def test_unexpected_error_hides_details_but_is_logged(caplog):
    with caplog.at_level(logging.ERROR):
        r = make_client().get("/boom")
    assert r.status_code == 500
    assert "secret" not in r.text
    assert "secret db password" in caplog.text  # still available for debugging


def test_json_log_line_has_service_and_request_id():
    token = request_id_var.set("req-123")
    try:
        record = logging.LogRecord("t", logging.INFO, __file__, 1, "hello %s", ("world",), None)
        line = json.loads(JsonFormatter("identity").format(record))
    finally:
        request_id_var.reset(token)
    assert line["message"] == "hello world"
    assert line["service"] == "identity"
    assert line["request_id"] == "req-123"
    assert line["level"] == "INFO"


def test_settings_read_from_env(monkeypatch):
    monkeypatch.setenv("SERVICE_NAME", "identity")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    s = BaseServiceSettings(_env_file=None)
    assert (s.service_name, s.log_level, s.environment) == ("identity", "DEBUG", "local")


def test_framework_http_errors_use_same_shape():
    r = make_client().get("/no-such-route")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "HTTP_404"


def test_request_id_generated_echoed_and_in_error_body():
    from smarthire_common.middleware import RequestIdMiddleware

    app = FastAPI()
    app.add_middleware(RequestIdMiddleware)
    register_exception_handlers(app)

    @app.get("/missing")
    async def missing():
        raise NotFoundError("nope")

    @app.get("/boom")
    async def boom():
        raise RuntimeError("x")

    client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/missing", headers={"X-Request-ID": "abc-1"})
    assert r.headers["X-Request-ID"] == "abc-1"  # caller's id is reused
    assert r.json()["error"]["request_id"] == "abc-1"  # and appears in the error body
    generated = client.get("/missing")
    assert generated.headers["X-Request-ID"]  # generated when absent
    assert client.get("/boom", headers={"X-Request-ID": "abc-2"}).json()["error"]["request_id"] == "abc-2"


def test_setup_logging_routes_uvicorn_through_json_handler():
    from smarthire_common.logging import setup_logging

    uvicorn_logger = logging.getLogger("uvicorn.access")
    uvicorn_logger.handlers = [logging.StreamHandler()]  # what uvicorn does at startup
    setup_logging("demo")
    assert uvicorn_logger.handlers == [] and uvicorn_logger.propagate
    assert isinstance(logging.getLogger().handlers[0].formatter, JsonFormatter)


def test_auth_and_rate_limit_errors_carry_headers():
    from smarthire_common.errors import ForbiddenError, TooManyRequestsError, UnauthorizedError

    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/401")
    async def e401():
        raise UnauthorizedError("Invalid or expired token")

    @app.get("/403")
    async def e403():
        raise ForbiddenError("Recruiters only")

    @app.get("/429")
    async def e429():
        raise TooManyRequestsError(retry_after=42)

    client = TestClient(app)
    r = client.get("/401")
    assert (r.status_code, r.headers["WWW-Authenticate"]) == (401, "Bearer")
    assert r.json()["error"]["code"] == "UNAUTHORIZED"
    assert client.get("/403").json()["error"]["code"] == "FORBIDDEN"
    r = client.get("/429")
    assert (r.status_code, r.headers["Retry-After"]) == (429, "42")


async def test_redis_helpers(monkeypatch):
    import fakeredis

    from smarthire_common import redis as sh_redis

    # Swap the real client class for an in-memory fake: no Redis server needed
    monkeypatch.setattr(sh_redis.Redis, "from_url", lambda url, **kw: fakeredis.FakeAsyncRedis(**kw))
    sh_redis.init_redis("redis://unused")
    await sh_redis.redis_ping_check()
    await sh_redis.get_redis().set("k", "v")
    assert await sh_redis.get_redis().get("k") == "v"
    await sh_redis.close_redis()
