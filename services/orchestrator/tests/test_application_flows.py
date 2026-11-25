"""Step T9: ApplicationWorkflow and ApplicationActionWorkflow through POST /actions, against a real local
Temporal dev server and fake Application, Scoring, Analytics, Identity and Notification."""

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
TASK_QUEUE = "application-flows-tq"
ERR = lambda status, code: httpx.Response(status, json={"error": {"code": code, "message": code}})  # noqa: E731


class Fakes:
    def __init__(self) -> None:
        self.applications: dict[str, dict] = {}
        self.scores: dict[str, float | None] = {}
        self.score_answer: dict[str, httpx.Response] = {}  # application id -> forced answer from Scoring
        self.facts: list[dict] = []
        self.emails: dict[str, dict] = {}

    def answer(self, app_id: str) -> httpx.Response:
        a = self.applications[app_id]
        return httpx.Response(200, json={"application": a, "context": {"job_title": "SRE", "recruiter_id": "r",
                                                                       "min_experience_years": 2}})

    async def handler(self, request: httpx.Request) -> httpx.Response:
        host, path = request.url.host, request.url.path
        body = json.loads(request.content) if request.content else {}
        parts = path.split("/")
        if host == "application":
            if path == "/internal/applications":
                existing = next((a for a in self.applications.values()
                                 if a["job_id"] == body["job_id"] and a["candidate_id"] == body["candidate_id"]), None)
                if existing and existing["id"] != body["id"]:
                    return ERR(409, "ALREADY_APPLIED")
                self.applications.setdefault(body["id"], {**body, "status": "SUBMITTED", "version": 1})
                return self.answer(body["id"])
            app_id, step = parts[3], parts[4]
            application = self.applications[app_id]
            if step == "score":
                self.scores[app_id] = body["score"]
                advanced = application["status"] == "SUBMITTED"
                application["status"] = "SCREENING" if advanced else application["status"]
                return httpx.Response(200, json={"advanced": advanced})
            application["status"] = {"stage": "IN_PROGRESS", "decision": body.get("decision"), "withdraw": "WITHDRAWN"}[step]
            return self.answer(app_id)
        if host == "scoring":
            forced = self.score_answer.get(body["application_id"])
            return forced or httpx.Response(200, json={"score": 82.5, "breakdown": {}})
        if host == "analytics":
            self.facts.append(body)
            return httpx.Response(200, json={"recorded": True})
        if host == "identity":
            return httpx.Response(200, json={"user_id": parts[3], "email": "c@example.com", "roles": [], "status": "ACTIVE"})
        if host == "notification":
            if path == "/internal/emails":
                self.emails[body["notification_key"]] = body
                return httpx.Response(200, json={"status": "QUEUED"})
            return httpx.Response(200, json={"status": "SENT"})
        return httpx.Response(404)

    def kinds(self, app_id: str) -> list[str]:
        return [f["kind"] for f in self.facts if f["data"].get("application_id") == app_id]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def setup():
    env = await WorkflowEnvironment.start_local(data_converter=encrypted_converter(os.urandom(32)))
    fakes = Fakes()
    signer = ServiceTokenSigner(rsa.generate_private_key(public_exponent=65537, key_size=2048))
    urls = {n: f"http://{n}" for n in ("application", "scoring", "analytics", "identity", "notification")}
    steps = ServiceSteps(urls, signer, httpx.AsyncClient(transport=httpx.MockTransport(fakes.handler)))
    settings = Settings(service_name="orchestrator", task_queue=TASK_QUEUE, temporal_enabled=False)
    async with Worker(env.client, task_queue=TASK_QUEUE, workflows=WORKFLOWS, activities=[steps.call_service]):
        app = create_app(settings, signer=signer, temporal_client=env.client)
        async with app.router.lifespan_context(app), httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://orchestrator") as client:
            yield client, fakes, env.client
    await env.shutdown()


def apply_body(job_id=None, candidate_id=None) -> dict:
    job_id, candidate_id = job_id or str(uuid.uuid4()), candidate_id or str(uuid.uuid4())
    application = {"id": str(uuid.uuid4()), "job_id": job_id, "candidate_id": candidate_id, "candidate_name": "Jane"}
    return {"key": {"job_id": job_id, "candidate_id": candidate_id}, "input": {"application": application}}


async def apply(client, body) -> httpx.Response:
    return await client.post("/actions/apply", json=body)


def workflow_of(body) -> str:
    return f"application-{body['key']['job_id']}-{body['key']['candidate_id']}"


async def test_apply_scores_and_screens(setup):
    client, fakes, temporal = setup
    body = apply_body()
    r = await apply(client, body)
    assert r.status_code == 200 and r.json()["result"]["application"]["status"] == "SUBMITTED"  # the 202 body
    app_id = body["input"]["application"]["id"]
    assert await temporal.get_workflow_handle(workflow_of(body)).result() == "SCREENING"
    assert fakes.scores[app_id] == 82.5
    assert fakes.kinds(app_id) == ["ApplicationSubmitted", "CandidateScored", "ApplicationStageChanged"]
    for template in ("APPLICATION_RECEIVED", "STAGE_CHANGED"):  # fire-and-forget children: wait for them
        email = f"email-{workflow_of(body)}-{template}"
        assert await temporal.get_workflow_handle(email).result() == "SENT"
        assert fakes.emails[email]["template_code"] == template


async def test_unscorable_is_pending_manual_and_reported(setup):
    client, fakes, temporal = setup
    body = apply_body()
    app_id = body["input"]["application"]["id"]
    fakes.score_answer[app_id] = ERR(422, "CANNOT_SCORE")
    await apply(client, body)
    assert await temporal.get_workflow_handle(workflow_of(body)).result() == "PENDING_MANUAL"
    assert fakes.scores[app_id] is None
    assert "SystemFailure" in [f["kind"] for f in fakes.facts if f["data"].get("source") == "application_workflow"]


async def test_another_apply_for_the_same_job_is_409(setup):
    client, fakes, temporal = setup
    first = apply_body()
    fakes.score_answer[first["input"]["application"]["id"]] = ERR(503, "RESUME_NOT_PARSED")  # keep it running
    assert (await apply(client, first)).status_code == 200
    second = apply_body(first["key"]["job_id"], first["key"]["candidate_id"])  # another key: another id
    r = await apply(client, second)
    assert r.status_code == 409 and r.json()["error"]["code"] == "ALREADY_APPLIED"
    retry = await apply(client, first)  # the SAME apply again: attaches and gets the first answer
    assert retry.status_code == 200 and retry.json()["result"]["application"]["id"] == first["input"]["application"]["id"]
    await temporal.get_workflow_handle(workflow_of(first)).terminate("test done")


async def test_withdraw_stops_scoring(setup):
    client, fakes, temporal = setup
    body = apply_body()
    app_id = body["input"]["application"]["id"]
    fakes.score_answer[app_id] = ERR(503, "RESUME_NOT_PARSED")  # scoring keeps waiting...
    await apply(client, body)
    await asyncio.sleep(1)
    r = await client.post("/actions/withdraw", json={"key": {"application_id": app_id}, "input": {
        "application_id": app_id, "job_id": body["key"]["job_id"], "candidate_id": body["key"]["candidate_id"]}})
    assert r.status_code == 200 and r.json()["result"]["application"]["status"] == "WITHDRAWN"
    assert await temporal.get_workflow_handle(workflow_of(body)).result() == "WITHDRAWN"  # ...until told to stop


async def test_decision_records_the_fact_and_tells_the_candidate(setup):
    client, fakes, temporal = setup
    body = apply_body()
    app_id = body["input"]["application"]["id"]
    await apply(client, body)
    await temporal.get_workflow_handle(workflow_of(body)).result()
    r = await client.post("/actions/decide", json={"key": {"application_id": app_id}, "input": {
        "application_id": app_id, "decision": "HIRED", "version": 2, "reason": None, "actor_id": str(uuid.uuid4())}})
    assert r.status_code == 200 and r.json()["result"]["application"]["status"] == "HIRED"
    assert await temporal.get_workflow_handle(f"decision-{app_id}").result() == "DONE"
    assert "ApplicationDecisionMade" in fakes.kinds(app_id)
    email_id = f"email-decision-{app_id}-DECISION_MADE"
    assert await temporal.get_workflow_handle(email_id).result() == "SENT"
    email = fakes.emails[email_id]
    assert email["variables"]["decision_message"].startswith("Congratulations! You have been selected for SRE")
