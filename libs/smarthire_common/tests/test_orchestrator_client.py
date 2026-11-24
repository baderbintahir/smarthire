"""Design v2.1: every outcome of POST /actions/{action} becomes one answer for the service's caller."""

import httpx
import pytest

from smarthire_common.errors import AppError, StillProcessingError, WorkflowsUnavailableError
from smarthire_common.orchestrator_client import OrchestratorClient


def client_with(handler) -> OrchestratorClient:
    return OrchestratorClient("http://orchestrator", http_client=httpx.AsyncClient(
        base_url="http://orchestrator", transport=httpx.MockTransport(handler)))


async def run(handler):
    return await client_with(handler).run_action("register", key={"k": 1}, input={"x": 2})


async def test_success_returns_the_commit_result():
    seen = {}

    def handler(request: httpx.Request):
        seen["path"], seen["body"] = request.url.path, request.read()
        return httpx.Response(200, json={"result": {"user_id": "u1"}})

    assert await run(handler) == {"user_id": "u1"}
    assert seen["path"] == "/actions/register"
    assert b'"key"' in seen["body"] and b'"input"' in seen["body"]


async def test_business_error_is_passed_through():
    def handler(request):
        return httpx.Response(409, json={"error": {"code": "EMAIL_TAKEN", "message": "Email already registered",
                                                   "request_id": "r", "details": {"field": "email"}}})

    with pytest.raises(AppError) as exc:
        await run(handler)
    assert (exc.value.http_status, exc.value.code, exc.value.details) == (409, "EMAIL_TAKEN", {"field": "email"})


async def test_cannot_connect_means_nothing_started():
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(WorkflowsUnavailableError) as exc:
        await run(handler)
    assert exc.value.http_status == 503 and exc.value.headers["Retry-After"] == "5"


async def test_lost_answer_means_still_processing():
    def handler(request):
        raise httpx.ReadTimeout("no answer", request=request)

    with pytest.raises(StillProcessingError):
        await run(handler)


async def test_orchestrator_still_processing_is_passed_on():
    def handler(request):
        return httpx.Response(503, json={"error": {"code": "STILL_PROCESSING", "message": "m", "request_id": "r"}})

    with pytest.raises(StillProcessingError):
        await run(handler)


async def test_orchestrator_cannot_reach_temporal_is_unavailable():
    def handler(request):
        return httpx.Response(503, json={"error": {"code": "WORKFLOWS_UNAVAILABLE", "message": "m", "request_id": "r"}})

    with pytest.raises(WorkflowsUnavailableError):
        await run(handler)


async def test_unknown_5xx_is_still_processing_not_nothing_happened():
    def handler(request):
        return httpx.Response(502, text="bad gateway")

    with pytest.raises(StillProcessingError):
        await run(handler)
