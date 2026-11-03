"""Steps 9.4-9.5: the REAL JobPublishingWorkflow and Job's REAL activities (on Postgres), run by
Temporal's time-skipping test server (retry back-offs pass instantly). Job Enrichment is replaced by
fake activities with the same names on enrichment-tq, so failures can be switched on."""

import uuid

import pytest
import pytest_asyncio
from smarthire_common import db, testing
from smarthire_common.events import new_event
from smarthire_common.kafka import RetryLater
from sqlalchemy import text
from temporalio import activity
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Worker

from app.events import handlers
from app.models import Job, JobStage
from app.workflows import activities
from app.workflows.publishing import ENRICHMENT_TQ, JOB_TQ, JobPublishingWorkflow

pytestmark = pytest.mark.asyncio(loop_scope="module")  # one event loop: Temporal env + DB engine share it

TABLES = ["jobs", "job_stages", "outbox", "processed_events"]


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def env():
    environment = await WorkflowEnvironment.start_time_skipping()
    yield environment
    await environment.shutdown()


@pytest_asyncio.fixture(loop_scope="module")
async def database(migrated, database_url):
    await testing.truncate(database_url, TABLES)
    db.init_db(database_url)  # no app runs in this module, so the chassis engine is ours
    yield
    await db.close_db()


async def processing_job(version: int = 2) -> str:
    """A job as the publish endpoint leaves it: PROCESSING at a new version."""
    async with db.session_scope() as s:
        job = Job(recruiter_id=uuid.uuid4(), title="Backend Engineer", description="Build APIs with Python and Kafka.",
                  employment_type="FULL_TIME", required_skills=["python"], status="PROCESSING", version=version)
        s.add(job)
        await s.flush()
        s.add_all([JobStage(job_id=job.id, name="Screening", position=0), JobStage(job_id=job.id, name="Offer", position=1)])
        return str(job.id)


async def row(job_id: str):
    async with db.session_scope() as s:
        return (await s.execute(text("SELECT status, version, published_at FROM jobs WHERE id = :id"), {"id": job_id})).one()


async def events() -> list[dict]:
    async with db.session_scope() as s:
        return [r[0] for r in (await s.execute(text("SELECT payload FROM outbox ORDER BY id"))).all()]


class FakeEnrichment:
    """Records calls; `broken` names an activity that always fails."""

    def __init__(self, broken: str | None = None):
        self.calls: list[str] = []
        self.stored: set[str] = set()
        self.broken = broken

    def _call(self, name):
        self.calls.append(name)
        if name == self.broken:
            raise RuntimeError(f"{name} is broken")

    def activities(self):
        @activity.defn(name="extract_skills")
        async def extract_skills(job_id: str, version: int, description: str, required: list[str]) -> dict:
            self._call("extract_skills")
            return {"skills": required + ["kafka"], "keywords": ["apis"]}

        @activity.defn(name="breakdown_jd")
        async def breakdown_jd(description: str) -> dict:
            self._call("breakdown_jd")
            return {"requirements": [], "responsibilities": [description], "benefits": []}

        @activity.defn(name="store_structured_job")
        async def store_structured_job(job_id: str, version: int, data: dict) -> None:
            self._call("store_structured_job")
            self.stored.add(f"{job_id}:{version}")

        @activity.defn(name="delete_structured_job")
        async def delete_structured_job(job_id: str, version: int) -> None:
            self._call("delete_structured_job")
            self.stored.discard(f"{job_id}:{version}")

        return [extract_skills, breakdown_jd, store_structured_job, delete_structured_job]


async def publish(env, job_id: str, version: int, enrichment: FakeEnrichment, job_activities=None) -> str:
    job_worker = Worker(env.client, task_queue=JOB_TQ, workflows=[JobPublishingWorkflow],
                        activities=job_activities or activities.ACTIVITIES)
    enrichment_worker = Worker(env.client, task_queue=ENRICHMENT_TQ, activities=enrichment.activities())
    async with job_worker, enrichment_worker:
        return await env.client.execute_workflow(JobPublishingWorkflow.run, args=[job_id, version],
                                                 id=handlers.workflow_id(job_id, version), task_queue=JOB_TQ)


# ---------- the saga ----------


async def test_happy_path_ends_ready_with_job_ready_event(env, database):
    job_id, enrichment = await processing_job(), FakeEnrichment()
    assert await publish(env, job_id, 2, enrichment) == "READY"
    assert enrichment.calls == ["extract_skills", "breakdown_jd", "store_structured_job"]
    assert enrichment.stored == {f"{job_id}:2"}
    status, version, published_at = await row(job_id)
    assert (status, version) == ("READY", 3) and published_at is not None
    [ready] = await events()
    assert ready["event_type"] == "JobReady"
    assert ready["payload"]["status"] == "READY" and ready["payload"]["version"] == 3
    assert ready["payload"]["stages"] == ["Screening", "Offer"]


async def test_failure_before_storing_marks_failed_without_compensation(env, database):
    job_id, enrichment = await processing_job(), FakeEnrichment(broken="extract_skills")
    assert await publish(env, job_id, 2, enrichment) == "FAILED"
    assert enrichment.calls == ["extract_skills"] * 5  # the retry policy: 5 attempts
    assert "delete_structured_job" not in enrichment.calls  # nothing was stored, nothing to undo
    assert (await row(job_id))[:2] == ("FAILED", 3)
    [failed] = await events()
    assert failed["event_type"] == "JobPublishFailed" and "extract_skills" in failed["payload"]["reason"]


async def test_failure_after_storing_compensates_then_marks_failed(env, database):
    job_id, enrichment = await processing_job(), FakeEnrichment()

    @activity.defn(name="mark_ready")
    async def broken_mark_ready(job_id: str, version: int) -> None:
        raise RuntimeError("database hiccup")

    result = await publish(env, job_id, 2, enrichment,
                           job_activities=[activities.set_processing, broken_mark_ready, activities.mark_failed])
    assert result == "FAILED"
    assert enrichment.calls[-1] == "delete_structured_job"  # undone, after mark_ready gave up
    assert enrichment.stored == set()  # the structured document is gone again
    assert (await row(job_id))[0] == "FAILED"


async def test_stale_version_does_nothing(env, database):
    """The job moved on (edited and re-published as v3) before this v2 run started."""
    job_id, enrichment = await processing_job(version=3), FakeEnrichment()
    assert await publish(env, job_id, 2, enrichment) == "SUPERSEDED"
    assert enrichment.calls == [] and (await row(job_id))[:2] == ("PROCESSING", 3) and await events() == []


# ---------- activities are idempotent ----------


async def test_mark_ready_twice_is_one_change_and_one_event(database):
    job_id = await processing_job()
    await ActivityEnvironment().run(activities.mark_ready, job_id, 2)
    await ActivityEnvironment().run(activities.mark_ready, job_id, 2)  # Temporal re-ran it after a lost result
    assert (await row(job_id))[:2] == ("READY", 3)
    assert [e["event_type"] for e in await events()] == ["JobReady"]


# ---------- the starter (JobPublishRequested consumer) ----------


class FakeTemporal:
    def __init__(self, error=None):
        self.started: list[str] = []
        self.error = error

    async def start_workflow(self, workflow, *, args, id, task_queue, id_reuse_policy):
        if self.error:
            raise self.error
        self.started.append(id)


@pytest.mark.parametrize("error", [None, WorkflowAlreadyStartedError("publish-x-2", "JobPublishingWorkflow")])
async def test_starter_starts_once_and_ignores_duplicates(monkeypatch, error):
    fake = FakeTemporal(error)

    async def connect(settings):
        return fake

    monkeypatch.setattr(handlers, "connect", connect)
    handler = handlers.build_handlers(settings=None)["JobPublishRequested"]
    await handler(None, new_event("JobPublishRequested", producer="job", payload={"job_id": "j1", "version": 2}))
    assert fake.started == ([] if error else ["publish-j1-2"])  # a duplicate start is not an error


async def test_starter_waits_when_temporal_is_down(monkeypatch):
    fake = FakeTemporal(RPCError("unavailable", RPCStatusCode.UNAVAILABLE, b""))

    async def connect(settings):
        return fake

    monkeypatch.setattr(handlers, "connect", connect)
    handler = handlers.build_handlers(settings=None)["JobPublishRequested"]
    with pytest.raises(RetryLater):  # the consumer retries later instead of dead-lettering the command
        await handler(None, new_event("JobPublishRequested", producer="job", payload={"job_id": "j1", "version": 2}))
