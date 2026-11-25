"""Step T5: SendEmailWorkflow against fake Identity and Notification internal endpoints."""

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
from app.steps import ServiceSteps
from app.workflows.email import SendEmailWorkflow
from tests.sample_workflows import NotifyingWorkflow

pytestmark = pytest.mark.asyncio(loop_scope="module")
TASK_QUEUE = "email-test-tq"


class FakeServices:
    def __init__(self) -> None:
        self.users: dict[str, dict] = {}
        self.emails: dict[str, dict] = {}
        self.send_answers: list[int] = []  # statuses to answer to the next sends (then 200)
        self.calls: list[str] = []

    async def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append(f"{request.method} {request.url.host}{path}")
        if request.url.host == "identity":
            user = self.users.get(path.split("/")[3])
            return httpx.Response(200, json=user) if user else httpx.Response(
                404, json={"error": {"code": "USER_NOT_FOUND", "message": "User not found"}})
        if path == "/internal/emails":
            body = json.loads(request.content)
            self.emails.setdefault(body["notification_key"], {**body, "status": "QUEUED"})
            return httpx.Response(200, json={"status": "QUEUED"})
        key = path.split("/")[3]
        if path.endswith("/send"):
            status = self.send_answers.pop(0) if self.send_answers else 200
            if status != 200:
                return httpx.Response(status, json={"error": {"code": "X", "message": "x"}})
            self.emails[key]["status"] = "SENT"
            return httpx.Response(200, json={"status": "SENT"})
        if path.endswith("/failed"):
            self.emails[key]["status"] = "FAILED"
            return httpx.Response(200, json={"status": "FAILED"})
        return httpx.Response(404)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def setup():
    env = await WorkflowEnvironment.start_local(data_converter=encrypted_converter(os.urandom(32)))
    fake = FakeServices()
    signer = ServiceTokenSigner(rsa.generate_private_key(public_exponent=65537, key_size=2048))
    steps = ServiceSteps({"identity": "http://identity", "notification": "http://notification"}, signer,
                         httpx.AsyncClient(transport=httpx.MockTransport(fake.handler)))
    async with Worker(env.client, task_queue=TASK_QUEUE, workflows=[SendEmailWorkflow, NotifyingWorkflow],
                      activities=[steps.call_service]):
        yield env.client, fake
    await env.shutdown()


def user(fake, status="ACTIVE") -> str:
    user_id = str(uuid.uuid4())
    fake.users[user_id] = {"user_id": user_id, "email": f"{user_id[:8]}@example.com", "roles": ["CANDIDATE"],
                           "status": status}
    return user_id


async def email(client, user_id, template="STAGE_CHANGED") -> tuple[str, str]:
    workflow_id = f"email-test-{uuid.uuid4()}"
    result = await client.execute_workflow(
        "SendEmailWorkflow", {"template": template, "user_id": user_id, "variables": {"job_title": "SRE"}},
        id=workflow_id, task_queue=TASK_QUEUE)
    return workflow_id, result


async def test_sends_to_an_active_user(setup):
    client, fake = setup
    workflow_id, result = await email(client, user(fake))
    assert result == "SENT" and fake.emails[workflow_id]["status"] == "SENT"
    assert fake.emails[workflow_id]["recipient"].endswith("@example.com")  # looked up, never passed in


async def test_suspended_or_missing_users_get_no_email(setup):
    client, fake = setup
    for user_id in (user(fake, status="SUSPENDED"), str(uuid.uuid4())):
        workflow_id, result = await email(client, user_id)
        assert result == "NO_RECIPIENT" and workflow_id not in fake.emails


async def test_smtp_down_is_retried_until_it_works(setup):
    client, fake = setup
    fake.send_answers = [503]  # one failed attempt, then SMTP is back (5 s backoff)
    workflow_id, result = await email(client, user(fake))
    assert result == "SENT"


async def test_a_send_that_can_never_work_is_marked_failed(setup):
    client, fake = setup
    fake.send_answers = [422]  # a business answer: retrying can't help
    workflow_id, result = await email(client, user(fake))
    assert result == "FAILED" and fake.emails[workflow_id]["status"] == "FAILED"


async def test_parent_finishes_and_the_email_still_goes(setup):
    client, fake = setup
    user_id = user(fake)
    parent_id = f"parent-{uuid.uuid4()}"
    assert await client.execute_workflow("NotifyingWorkflow", {"user_id": user_id}, id=parent_id,
                                         task_queue=TASK_QUEUE) == "PARENT_DONE"
    child = client.get_workflow_handle(f"email-{parent_id}-JOB_READY")
    assert await child.result() == "SENT"
