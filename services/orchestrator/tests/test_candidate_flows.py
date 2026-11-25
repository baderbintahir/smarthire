"""Step T7: ResumeProcessingWorkflow through POST /actions (fake Candidate and candidate-parser)."""

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
TASK_QUEUE = "candidate-flows-tq"


class FakeCandidate:
    def __init__(self) -> None:
        self.resumes: dict[str, dict] = {}
        self.unreadable: set[str] = set()
        self.profiles: set[str] = set()

    async def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.url.host == "candidate" and path == "/internal/resumes":
            body = json.loads(request.content)
            if body["candidate_id"] not in self.profiles:
                return httpx.Response(404, json={"error": {"code": "CANDIDATE_NOT_FOUND", "message": "no profile"}})
            self.resumes.setdefault(body["id"], {**body, "status": "UPLOADED"})
            return httpx.Response(200, json=self.resumes[body["id"]])
        if request.url.host == "candidate-parser" and path.endswith("/parse"):
            resume = self.resumes[path.split("/")[3]]
            if resume["id"] in self.unreadable:
                resume["status"] = "FAILED"
                return httpx.Response(422, json={"error": {"code": "RESUME_UNREADABLE", "message": "unreadable"}})
            resume["status"] = "PARSED"
            return httpx.Response(200, json={"status": "PARSED"})
        return httpx.Response(404)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def setup():
    env = await WorkflowEnvironment.start_local(data_converter=encrypted_converter(os.urandom(32)))
    fake = FakeCandidate()
    signer = ServiceTokenSigner(rsa.generate_private_key(public_exponent=65537, key_size=2048))
    steps = ServiceSteps({"candidate": "http://candidate", "candidate_parser": "http://candidate-parser"}, signer,
                         httpx.AsyncClient(transport=httpx.MockTransport(fake.handler)))
    settings = Settings(service_name="orchestrator", task_queue=TASK_QUEUE, temporal_enabled=False)
    async with Worker(env.client, task_queue=TASK_QUEUE, workflows=WORKFLOWS, activities=[steps.call_service]):
        app = create_app(settings, signer=signer, temporal_client=env.client)
        async with app.router.lifespan_context(app), httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://orchestrator") as client:
            yield client, fake, env.client
    await env.shutdown()


def upload(fake, *, profile=True) -> dict:
    candidate_id, resume_id = str(uuid.uuid4()), str(uuid.uuid4())
    if profile:
        fake.profiles.add(candidate_id)
    resume = {"id": resume_id, "candidate_id": candidate_id, "object_key": f"{candidate_id}/{resume_id}",
              "file_name": "cv.pdf", "content_type": "application/pdf", "size_bytes": 1234}
    return {"key": {"resume_id": resume_id}, "input": {"resume": resume}}


async def test_upload_is_registered_then_parsed(setup):
    client, fake, temporal = setup
    body = upload(fake)
    r = await client.post("/actions/upload-resume", json=body)
    assert r.status_code == 200 and r.json()["result"]["status"] == "UPLOADED"  # the caller's 202 body
    resume_id = body["key"]["resume_id"]
    assert await temporal.get_workflow_handle(f"resume-{resume_id}").result() == "PARSED"
    assert fake.resumes[resume_id]["status"] == "PARSED"


async def test_unreadable_file_ends_without_retries(setup):
    client, fake, temporal = setup
    body = upload(fake)
    fake.unreadable.add(body["key"]["resume_id"])
    assert (await client.post("/actions/upload-resume", json=body)).status_code == 200
    assert await temporal.get_workflow_handle(f"resume-{body['key']['resume_id']}").result() == "UNREADABLE"


async def test_no_profile_is_404_and_nothing_is_parsed(setup):
    client, fake, temporal = setup
    body = upload(fake, profile=False)
    r = await client.post("/actions/upload-resume", json=body)
    assert r.status_code == 404 and r.json()["error"]["code"] == "CANDIDATE_NOT_FOUND"
    assert await temporal.get_workflow_handle(f"resume-{body['key']['resume_id']}").result() == "REJECTED"
