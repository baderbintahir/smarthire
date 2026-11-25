"""Step T3 "Done when": the spike's five checks, now through POST /actions and HTTP steps, against a real
local Temporal dev server (downloaded on first use) and fake internal endpoints."""

import asyncio
import json
import os

import httpx
import jwt
import pytest
import pytest_asyncio
from cryptography.hazmat.primitives.asymmetric import rsa
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.workflowservice.v1 import GetWorkflowExecutionHistoryRequest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from smarthire_common.service_auth import AUDIENCE, ISSUER, ServiceTokenSigner

from app.actions import Action
from app.codec import ENCODING, encrypted_converter
from app.config import Settings
from app.main import create_app
from app.steps import ServiceSteps
from tests.sample_workflows import ThingWorkflow

pytestmark = pytest.mark.asyncio(loop_scope="module")
TASK_QUEUE = "orchestrator-test-tq"


class FakeThings:
    """Stands in for a service's internal endpoints; checks every call like a real service would."""

    def __init__(self, signer: ServiceTokenSigner) -> None:
        self.public_key = signer._key.public_key()
        self.made: dict[str, dict] = {}
        self.thing_calls: list[tuple[str, str]] = []  # (id, Idempotency-Key)
        self.after: list[str] = []
        self.fail_next = 0
        self.delay = 0.0

    async def handler(self, request: httpx.Request) -> httpx.Response:
        token = request.headers["Authorization"].removeprefix("Bearer ")
        jwt.decode(token, self.public_key, algorithms=["RS256"], issuer=ISSUER, audience=AUDIENCE)  # raises if bad
        key = request.headers["Idempotency-Key"]
        body = json.loads(request.content or b"{}")
        if request.url.path == "/internal/things":
            self.thing_calls.append((body["id"], key))
            if self.fail_next:
                self.fail_next -= 1
                return httpx.Response(500, json={"error": {"code": "INTERNAL_ERROR", "message": "boom"}})
            if self.delay:
                await asyncio.sleep(self.delay)
            if body["name"] == "duplicate":
                return httpx.Response(409, json={"error": {"code": "DUPLICATE_THING", "message": "Already made",
                                                           "details": {"id": body["id"]}}})
            return httpx.Response(200, json=self.made.setdefault(body["id"], {"id": body["id"], "name": body["name"]}))
        if request.url.path == "/internal/after":
            self.after.append(body["id"])
            return httpx.Response(200, json={})
        return httpx.Response(404)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def env():
    environment = await WorkflowEnvironment.start_local(data_converter=encrypted_converter(os.urandom(32)))
    yield environment
    await environment.shutdown()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def setup(env):
    signer = ServiceTokenSigner(rsa.generate_private_key(public_exponent=65537, key_size=2048))
    things = FakeThings(signer)
    http = httpx.AsyncClient(transport=httpx.MockTransport(things.handler))
    steps = ServiceSteps({"things": "http://things"}, signer, http)
    settings = Settings(service_name="orchestrator", task_queue=TASK_QUEUE, commit_wait_seconds=5, temporal_enabled=False)
    actions = {"make-thing": Action("ThingWorkflow", lambda key: f"thing-{key['id']}")}
    async with Worker(env.client, task_queue=TASK_QUEUE, workflows=[ThingWorkflow], activities=[steps.call_service]):
        app = create_app(settings, signer=signer, temporal_client=env.client, action_table=actions)
        async with app.router.lifespan_context(app), httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://orchestrator") as client:
            yield client, things, env.client, signer


def make(thing_id: str, name: str = "book") -> dict:
    return {"key": {"id": thing_id}, "input": {"id": thing_id, "name": name}}


async def test_commit_answers_and_follow_up_runs(setup):
    client, things, temporal, _ = setup
    r = await client.post("/actions/make-thing", json=make("a"))
    assert r.status_code == 200
    assert r.json() == {"result": {"id": "a", "name": "book"}}
    assert await temporal.get_workflow_handle("thing-a").result() == "DONE"
    assert "a" in things.after


async def test_business_error_reaches_the_caller_and_ends_the_workflow(setup):
    client, things, temporal, _ = setup
    r = await client.post("/actions/make-thing", json=make("dup", name="duplicate"))
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "DUPLICATE_THING"
    assert r.json()["error"]["details"] == {"id": "dup"}
    assert await temporal.get_workflow_handle("thing-dup").result() == "REJECTED"
    assert "dup" not in things.after  # no follow-up after a rejected commit


async def test_same_action_twice_meets_one_workflow_and_commits_once(setup):
    client, things, _, _ = setup
    things.delay = 1.0  # keep the commit busy so the three calls really overlap
    try:
        answers = await asyncio.gather(*(client.post("/actions/make-thing", json=make("p")) for _ in range(3)))
    finally:
        things.delay = 0.0
    assert [a.status_code for a in answers] == [200, 200, 200]
    assert len({json.dumps(a.json()) for a in answers}) == 1
    assert [i for i, _ in things.thing_calls].count("p") == 1  # the commit endpoint was called once


async def test_failing_service_is_retried_with_the_same_idempotency_key(setup):
    client, things, _, _ = setup
    things.fail_next = 2  # two 500s, then success
    r = await client.post("/actions/make-thing", json=make("retry"))
    assert r.status_code == 200
    keys = [key for i, key in things.thing_calls if i == "retry"]
    assert len(keys) == 3 and len(set(keys)) == 1 and keys[0].startswith("thing-retry:")


async def test_no_answer_in_time_is_still_processing_then_finishes(setup):
    client, things, temporal, _ = setup
    things.delay = 7.0  # longer than commit_wait_seconds (5)
    try:
        r = await client.post("/actions/make-thing", json=make("slow"))
    finally:
        things.delay = 0.0
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "STILL_PROCESSING"
    assert await temporal.get_workflow_handle("thing-slow").result() == "DONE"  # it finished anyway
    assert "slow" in things.made


async def test_temporal_unreachable_is_workflows_unavailable(setup):
    _, _, _, signer = setup
    app = create_app(Settings(service_name="orchestrator", temporal_enabled=False), signer=signer,
                     temporal_client=None, action_table={"make-thing": Action("ThingWorkflow", lambda k: "x")})
    async with app.router.lifespan_context(app), httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://o") as client:
        r = await client.post("/actions/make-thing", json=make("never"))
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "WORKFLOWS_UNAVAILABLE"
    assert r.headers["Retry-After"] == "5"


async def test_unknown_action_is_404(setup):
    client, _, _, _ = setup
    r = await client.post("/actions/nope", json=make("n"))
    assert r.status_code == 404


async def test_temporal_stores_only_ciphertext(setup):
    client, _, temporal, _ = setup
    await client.post("/actions/make-thing", json=make("secret", name="a very private name"))
    history = await temporal.workflow_service.get_workflow_execution_history(GetWorkflowExecutionHistoryRequest(
        namespace=temporal.namespace, execution=WorkflowExecution(workflow_id="thing-secret")))
    started = history.history.events[0].workflow_execution_started_event_attributes
    payload = started.input.payloads[0]
    assert payload.metadata["encoding"] == ENCODING
    assert b"private" not in payload.data


async def test_jwks_publishes_the_service_token_key(setup):
    client, _, _, signer = setup
    r = await client.get("/.well-known/jwks.json")
    assert r.json() == signer.jwks()


async def test_a_retry_after_the_action_finished_gets_the_same_answer_without_a_second_run(setup):
    """A caller whose answer was lost retries after the workflow already finished: it gets the stored
    answer, and the follow-up steps (emails, facts in real workflows) do not run again."""
    client, things, temporal, _ = setup
    first = await client.post("/actions/make-thing", json=make("done"))
    assert await temporal.get_workflow_handle("thing-done").result() == "DONE"
    again = await client.post("/actions/make-thing", json=make("done"))
    assert again.status_code == 200 and again.json() == first.json()
    assert things.after.count("done") == 1  # the follow-up ran once
    rejected = await client.post("/actions/make-thing", json=make("dup2", name="duplicate"))
    await temporal.get_workflow_handle("thing-dup2").result()
    again = await client.post("/actions/make-thing", json=make("dup2", name="duplicate"))
    assert again.status_code == rejected.status_code == 409  # a stored "no" is answered again too
