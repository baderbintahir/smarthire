"""Step T8: JobPublishingWorkflow (the saga) and JobClosingWorkflow through POST /actions, against a real
local Temporal dev server and fake Job, Enrichment, Application, Analytics, Identity and Notification."""

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
TASK_QUEUE = "job-flows-tq"
ERR = lambda status, code: httpx.Response(status, json={"error": {"code": code, "message": code}})  # noqa: E731


class Fakes:
    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {}
        self.structured: set[tuple[str, int]] = set()
        self.deleted: list[tuple[str, int]] = []
        self.replica: dict[str, list[str]] = {}  # job id -> statuses synced, in order
        self.facts: list[dict] = []
        self.emails: dict[str, dict] = {}
        self.enrichment_down: set[str] = set()  # job ids whose enrichment always fails
        self.ready_answer: dict[str, httpx.Response] = {}  # job id -> forced answer to /ready
        self.closed_replicas: list[str] = []

    def job(self, status="DRAFT") -> str:
        job_id = str(uuid.uuid4())
        self.jobs[job_id] = {"job_id": job_id, "recruiter_id": str(uuid.uuid4()), "title": "SRE", "status": status,
                             "version": 1, "stages": ["Interview"], "eligibility_rules": {}, "application_limit": None,
                             "deadline": None, "description": "Run Kafka in production", "required_skills": ["kafka"]}
        return job_id

    def _move(self, job_id: str, status: str) -> httpx.Response:
        job = self.jobs[job_id]
        job["status"], job["version"] = status, job["version"] + 1
        return httpx.Response(200, json=dict(job))

    async def handler(self, request: httpx.Request) -> httpx.Response:
        host, path = request.url.host, request.url.path
        body = json.loads(request.content) if request.content else {}
        parts = path.split("/")
        if host == "job":
            job_id, step = parts[3], parts[4]
            job = self.jobs[job_id]
            if step == "processing":
                if job["version"] != body["expected_version"]:
                    return ERR(409, "VERSION_CONFLICT")
                return self._move(job_id, "PROCESSING")
            if step == "ready":
                return self.ready_answer.get(job_id) or self._move(job_id, "READY")
            if step == "failed":
                return self._move(job_id, "FAILED")
            if step == "close":
                return self._move(job_id, "CLOSED")
        if host == "enrichment":
            if request.method == "DELETE":
                self.deleted.append((parts[3], int(parts[4])))
                return httpx.Response(204)
            if body["job_id"] in self.enrichment_down:
                return ERR(500, "INTERNAL_ERROR")
            self.structured.add((body["job_id"], body["version"]))
            return httpx.Response(200, json={"skills": ["kafka"]})
        if host == "application":
            if path.endswith("/close"):
                self.closed_replicas.append(parts[3])
                return httpx.Response(200, json={"closed": [{"application_id": str(uuid.uuid4()),
                                                             "candidate_id": str(uuid.uuid4())}]})
            self.replica.setdefault(parts[3], []).append(body["status"])
            return httpx.Response(200, json={"updated": True})
        if host == "analytics":
            self.facts.append(body)
            return httpx.Response(200, json={"recorded": True})
        if host == "identity":
            return httpx.Response(200, json={"user_id": parts[3], "email": "r@example.com", "roles": [], "status": "ACTIVE"})
        if host == "notification":
            if path == "/internal/emails":
                self.emails[body["notification_key"]] = body
                return httpx.Response(200, json={"status": "QUEUED"})
            return httpx.Response(200, json={"status": "SENT"})
        return httpx.Response(404)

    def fact(self, kind: str, job_id: str) -> dict | None:
        return next((f for f in self.facts if f["kind"] == kind and f["data"]["job_id"] == job_id), None)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def setup():
    env = await WorkflowEnvironment.start_local(data_converter=encrypted_converter(os.urandom(32)))
    fakes = Fakes()
    signer = ServiceTokenSigner(rsa.generate_private_key(public_exponent=65537, key_size=2048))
    urls = {name: f"http://{name}" for name in ("job", "enrichment", "application", "analytics", "identity", "notification")}
    steps = ServiceSteps(urls, signer, httpx.AsyncClient(transport=httpx.MockTransport(fakes.handler)))
    settings = Settings(service_name="orchestrator", task_queue=TASK_QUEUE, temporal_enabled=False)
    async with Worker(env.client, task_queue=TASK_QUEUE, workflows=WORKFLOWS, activities=[steps.call_service]):
        app = create_app(settings, signer=signer, temporal_client=env.client)
        async with app.router.lifespan_context(app), httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://orchestrator") as client:
            yield client, fakes, env.client
    await env.shutdown()


async def publish(client, temporal, job_id: str) -> tuple[httpx.Response, str]:
    r = await client.post("/actions/publish-job", json={"key": {"job_id": job_id, "version": 1},
                                                         "input": {"job_id": job_id, "expected_version": 1}})
    result = await temporal.get_workflow_handle(f"publish-{job_id}-v1").result() if r.status_code == 200 else None
    return r, result


async def test_publish_makes_the_job_ready_everywhere(setup):
    client, fakes, temporal = setup
    job_id = fakes.job()
    r, result = await publish(client, temporal, job_id)
    assert r.status_code == 200 and r.json()["result"]["status"] == "PROCESSING"  # the caller's 202
    assert result == "READY"
    assert fakes.replica[job_id] == ["PROCESSING", "READY"]  # nobody applied while it was processing
    assert (job_id, 2) in fakes.structured
    assert fakes.fact("JobReady", job_id)["data"] == {"job_id": job_id, "title": "SRE", "version": 3}
    email = f"email-publish-{job_id}-v1-JOB_READY"
    assert await temporal.get_workflow_handle(email).result() == "SENT"


async def test_failures_end_in_failed_with_compensation(setup):
    """Enrichment down: FAILED, nothing to undo. mark_ready failing after the document was stored: the
    document is deleted again (compensation), then FAILED. Both run at once (each takes ~15 s of retries)."""
    client, fakes, temporal = setup
    no_enrichment, no_ready = fakes.job(), fakes.job()
    fakes.enrichment_down.add(no_enrichment)
    fakes.ready_answer[no_ready] = ERR(500, "INTERNAL_ERROR")
    (_, first), (_, second) = await asyncio.gather(publish(client, temporal, no_enrichment),
                                                   publish(client, temporal, no_ready))
    assert first == "FAILED" and second == "FAILED"
    assert fakes.deleted == [(no_ready, 2)]  # only the one whose document was stored
    assert fakes.replica[no_enrichment] == ["PROCESSING", "FAILED"]
    assert fakes.fact("JobPublishFailed", no_ready) is not None
    email = f"email-publish-{no_ready}-v1-JOB_PUBLISH_FAILED"
    assert await temporal.get_workflow_handle(email).result() == "SENT"
    assert "mark_ready failed" in fakes.emails[email]["variables"]["reason"]


async def test_a_superseded_run_stops_quietly(setup):
    client, fakes, temporal = setup
    job_id = fakes.job()
    fakes.ready_answer[job_id] = ERR(409, "STALE_JOB_VERSION")  # the job was edited meanwhile
    _, result = await publish(client, temporal, job_id)
    assert result == "SUPERSEDED"
    assert fakes.deleted[-1] == (job_id, 2)  # its document is undone
    assert fakes.fact("JobPublishFailed", job_id) is None  # not a failure: a newer run owns the job


async def test_a_version_conflict_at_commit_is_409(setup):
    client, fakes, temporal = setup
    job_id = fakes.job()
    fakes.jobs[job_id]["version"] = 5  # someone saved after the recruiter loaded the job
    r, _ = await publish(client, temporal, job_id)
    assert r.status_code == 409 and r.json()["error"]["code"] == "VERSION_CONFLICT"
    assert job_id not in fakes.replica


async def test_close_closes_the_applications(setup):
    client, fakes, temporal = setup
    job_id = fakes.job(status="READY")
    r = await client.post("/actions/close-job", json={"key": {"job_id": job_id},
                                                       "input": {"job_id": job_id, "expected_version": 1}})
    assert r.status_code == 200 and r.json()["result"]["status"] == "CLOSED"
    assert await temporal.get_workflow_handle(f"close-{job_id}").result() == "DONE"
    assert fakes.closed_replicas == [job_id]
