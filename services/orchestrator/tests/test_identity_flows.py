"""Step T6: RegistrationWorkflow and RoleChangeWorkflow through POST /actions, against a real local
Temporal dev server and fake internal endpoints of Identity, Candidate and Analytics."""

import asyncio
import json
import os
import uuid

import httpx
import pytest
import pytest_asyncio
from cryptography.hazmat.primitives.asymmetric import rsa
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from smarthire_common.service_auth import ServiceTokenSigner

from app.codec import encrypted_converter
from app.config import Settings
from app.main import create_app
from app.steps import ServiceSteps
from app.workflows import WORKFLOWS

pytestmark = pytest.mark.asyncio(loop_scope="module")
TASK_QUEUE = "identity-flows-tq"


class FakeServices:
    def __init__(self) -> None:
        self.users: dict[str, dict] = {}  # id -> user
        self.profiles: list[str] = []
        self.facts: list[dict] = []
        self.profile_delay = 0.0

    async def handler(self, request: httpx.Request) -> httpx.Response:
        host, path, method = request.url.host, request.url.path, request.method
        body = json.loads(request.content) if request.content else None
        if host == "identity" and path == "/internal/users":
            if any(u["email"] == body["email"] and u["id"] != body["id"] for u in self.users.values()):
                return httpx.Response(409, json={"error": {"code": "EMAIL_ALREADY_REGISTERED", "message": "taken"}})
            self.users.setdefault(body["id"], {k: body[k] for k in ("id", "email", "roles")})
            return httpx.Response(200, json=self.users[body["id"]])
        if host == "identity" and path.startswith("/internal/users/"):
            user = self.users[path.split("/")[3]]
            if method == "POST":
                user["roles"] = sorted(set(user["roles"]) | {body["role"]})
                return httpx.Response(200, json=user)
            user["roles"] = [r for r in user["roles"] if r != path.split("/")[5]]
            return httpx.Response(204)
        if host == "candidate":
            await asyncio.sleep(self.profile_delay)
            self.profiles.append(path.split("/")[3])
            return httpx.Response(200, json={})
        if host == "analytics":
            if not any(f["fact_id"] == body["fact_id"] for f in self.facts):
                self.facts.append(body)
            return httpx.Response(200, json={"recorded": True})
        return httpx.Response(404)

    def facts_of(self, kind: str) -> list[dict]:
        return [f for f in self.facts if f["kind"] == kind]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def setup():
    env = await WorkflowEnvironment.start_local(data_converter=encrypted_converter(os.urandom(32)))
    fake = FakeServices()
    signer = ServiceTokenSigner(rsa.generate_private_key(public_exponent=65537, key_size=2048))
    steps = ServiceSteps({"identity": "http://identity", "candidate": "http://candidate", "analytics": "http://analytics"},
                         signer, httpx.AsyncClient(transport=httpx.MockTransport(fake.handler)))
    settings = Settings(service_name="orchestrator", task_queue=TASK_QUEUE, temporal_enabled=False)
    async with Worker(env.client, task_queue=TASK_QUEUE, workflows=WORKFLOWS, activities=[steps.call_service]):
        app = create_app(settings, signer=signer, temporal_client=env.client)  # the REAL action table
        async with app.router.lifespan_context(app), httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://orchestrator") as client:
            yield client, fake, env.client
    await env.shutdown()


def register_body(email: str, roles: list[str]) -> dict:
    user = {"id": str(uuid.uuid4()), "email": email, "password_hash": "$argon2id$x", "full_name": "Ana",
            "current_company": None, "roles": roles}
    return {"key": {"email_hash": uuid.uuid4().hex}, "input": {"user": user}}


async def finished(temporal, workflow_id: str) -> str:
    return await temporal.get_workflow_handle(workflow_id).result()


async def test_register_a_candidate(setup):
    client, fake, temporal = setup
    body = register_body("ana@example.com", ["CANDIDATE"])
    r = await client.post("/actions/register", json=body)
    assert r.status_code == 200 and r.json()["result"]["id"] == body["input"]["user"]["id"]
    assert await finished(temporal, f"register-{body['key']['email_hash']}") == "DONE"
    user_id = body["input"]["user"]["id"]
    assert user_id in fake.profiles
    [fact] = [f for f in fake.facts_of("UserRegistered") if f["data"]["user_id"] == user_id]
    assert fact["data"] == {"user_id": user_id, "role": "CANDIDATE", "roles": ["CANDIDATE"]}


async def test_a_recruiter_gets_no_candidate_profile(setup):
    client, fake, temporal = setup
    body = register_body("rita@example.com", ["RECRUITER"])
    assert (await client.post("/actions/register", json=body)).status_code == 200
    assert await finished(temporal, f"register-{body['key']['email_hash']}") == "DONE"
    assert body["input"]["user"]["id"] not in fake.profiles


async def test_email_taken_is_409_and_nothing_follows(setup):
    client, fake, temporal = setup
    first = register_body("taken@example.com", ["CANDIDATE"])
    await client.post("/actions/register", json=first)
    await finished(temporal, f"register-{first['key']['email_hash']}")
    second = register_body("taken@example.com", ["CANDIDATE"])  # another request, another workflow id
    r = await client.post("/actions/register", json=second)
    assert r.status_code == 409 and r.json()["error"]["code"] == "EMAIL_ALREADY_REGISTERED"
    assert await finished(temporal, f"register-{second['key']['email_hash']}") == "REJECTED"
    assert second["input"]["user"]["id"] not in fake.profiles


async def test_a_second_registration_attaching_to_a_running_one_is_409(setup):
    """Same email hash = same workflow id: the second request attaches, but it is not a retry."""
    client, fake, temporal = setup
    fake.profile_delay = 2.0  # keep the first workflow running while the second request comes in
    try:
        first = register_body("race@example.com", ["CANDIDATE"])
        second = register_body("race@example.com", ["CANDIDATE"])
        second["key"] = first["key"]
        assert (await client.post("/actions/register", json=first)).status_code == 200
        r = await client.post("/actions/register", json=second)
        assert r.status_code == 409 and r.json()["error"]["code"] == "EMAIL_ALREADY_REGISTERED"
        assert await finished(temporal, f"register-{first['key']['email_hash']}") == "DONE"
    finally:
        fake.profile_delay = 0.0


async def test_grant_revoke_grant_counts_each_change(setup):
    client, fake, temporal = setup
    body = register_body("multi@example.com", ["RECRUITER"])
    await client.post("/actions/register", json=body)
    user_id = body["input"]["user"]["id"]
    key = {"user_id": user_id, "role": "CANDIDATE"}
    grant = {"key": key, "input": {"user_id": user_id, "role": "CANDIDATE", "granted_by": None}}
    revoke = {"key": key, "input": {"user_id": user_id, "role": "CANDIDATE", "revoked_by": str(uuid.uuid4())}}

    r = await client.post("/actions/grant-role", json=grant)
    assert r.status_code == 200 and "CANDIDATE" in r.json()["result"]["roles"]
    assert await finished(temporal, f"role-{user_id}-CANDIDATE-grant") == "DONE"
    assert user_id in fake.profiles  # taking the CANDIDATE role creates the profile
    assert (await client.post("/actions/revoke-role", json=revoke)).status_code == 200
    assert await finished(temporal, f"role-{user_id}-CANDIDATE-revoke") == "DONE"
    assert (await client.post("/actions/grant-role", json=grant)).status_code == 200  # the same id, a new run
    assert await finished(temporal, f"role-{user_id}-CANDIDATE-grant") == "DONE"

    grants = [f for f in fake.facts_of("RoleGranted") if f["data"]["user_id"] == user_id]
    revokes = [f for f in fake.facts_of("RoleRevoked") if f["data"]["user_id"] == user_id]
    assert len(grants) == 2 and len({f["fact_id"] for f in grants}) == 2  # two runs: two facts, not one
    assert len(revokes) == 1
