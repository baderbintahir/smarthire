"""Steps 10.4-10.5: the REAL ApplicationWorkflow and Application's REAL activities (on Postgres), run by
Temporal's time-skipping test server. Scoring is a fake activity with the same name on scoring-tq."""

import uuid
from datetime import timedelta

import pytest
import pytest_asyncio
from smarthire_common import db, testing
from smarthire_common.events import new_event
from sqlalchemy import text
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Worker

from app.events import handlers
from app.models import Application, ApplicationQuota, JobReplica
from app.services import workflows
from app.workflows import activities
from app.workflows.processing import APPLICATION_TQ, CANNOT_SCORE, SCORING_TQ, ApplicationWorkflow
from tests.conftest import TABLES

pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def env():
    environment = await WorkflowEnvironment.start_time_skipping()
    yield environment
    await environment.shutdown()


@pytest_asyncio.fixture(loop_scope="module")
async def database(migrated, database_url):
    await testing.truncate(database_url, TABLES)
    db.init_db(database_url)  # no app runs in this module
    yield
    await db.close_db()


async def submitted(status="SUBMITTED", stage="SUBMITTED", job_id=None) -> str:
    async with db.session_scope() as s:
        if job_id is None:
            job = JobReplica(job_id=uuid.uuid4(), recruiter_id=uuid.uuid4(), title="Backend Engineer", status="READY",
                             stages=["Interview"], eligibility_rules={"min_experience_years": 2}, source_version=1)
            s.add(job)
            await s.flush()
            job_id = job.job_id
        application = Application(job_id=job_id, candidate_id=uuid.uuid4(), candidate_name="Jane Doe", status=status,
                                  current_stage=stage)
        s.add(application)
        s.add(ApplicationQuota(candidate_id=application.candidate_id, active_count=1))
        await s.flush()
        return str(application.id)


async def fetch(sql: str, **params):
    async with db.session_scope() as s:
        return (await s.execute(text(sql), params)).all()


def scoring(result=None, error=None):
    calls = []

    @activity.defn(name="score_application")
    async def score_application(application_id: str, candidate_id: str, job_id: str, min_years) -> dict:
        calls.append(min_years)
        if error:
            raise error
        return result or {"score": 82.5, "breakdown": {"matched_skills": ["python"]}}

    return score_application, calls


def workers(env, score_activity=None):
    app_worker = Worker(env.client, task_queue=APPLICATION_TQ, workflows=[ApplicationWorkflow],
                        activities=activities.ACTIVITIES)
    if score_activity is None:
        return [app_worker]  # no Scoring worker at all (Scoring is down)
    return [app_worker, Worker(env.client, task_queue=SCORING_TQ, activities=[score_activity])]


async def run(env, application_id, score_activity=None) -> str:
    started = workers(env, score_activity)
    for worker in started:
        await worker.__aenter__()
    try:
        return await env.client.execute_workflow(ApplicationWorkflow.run, application_id,
                                                 id=workflows.workflow_id(application_id), task_queue=APPLICATION_TQ)
    finally:
        for worker in started:
            await worker.__aexit__(None, None, None)


# ---------- 10.4 the workflow ----------


async def test_scored_application_moves_to_screening(env, database):
    application_id = await submitted()
    score_activity, calls = scoring()
    assert await run(env, application_id, score_activity) == "SCREENING"
    assert calls == [2]  # the job's minimum experience was passed to Scoring
    [(status, stage, score, score_status)] = await fetch("SELECT status, current_stage, score, score_status FROM applications")
    assert (status, stage, float(score), score_status) == ("SCREENING", "SCREENING", 82.5, "SCORED")
    history = await fetch("SELECT from_stage, to_stage, changed_by FROM application_stage_history ORDER BY id")
    assert history == [("SUBMITTED", "SCREENING", activities.SYSTEM_ACTOR)]
    [(event,)] = await fetch("SELECT payload FROM outbox")
    assert event["event_type"] == "ApplicationStageChanged" and event["payload"]["to_stage"] == "SCREENING"


async def test_unscorable_application_is_pending_manual_but_still_screened(env, database):
    application_id = await submitted()
    score_activity, _ = scoring(error=ApplicationError("no resume", type=CANNOT_SCORE, non_retryable=True))
    assert await run(env, application_id, score_activity) == "PENDING_MANUAL"
    [(status, score, score_status)] = await fetch("SELECT status, score, score_status FROM applications")
    assert (status, score, score_status) == ("SCREENING", None, "PENDING_MANUAL")
    # Step 13.5: the give-up is reported, in the same transaction as PENDING_MANUAL
    events = {e["event_type"]: e["payload"] for (e,) in await fetch("SELECT payload FROM outbox")}
    assert events["SystemFailure"]["source"] == "application_workflow"
    assert events["SystemFailure"]["kind"] == "WORKFLOW_FAILED" and "no resume" in events["SystemFailure"]["detail"]


async def test_scoring_has_an_overall_deadline():
    """If Scoring stays down, scoring gives up after 10 minutes in total (then PENDING_MANUAL, the same
    path as test_unscorable_...). Temporal's test server can't fast-forward while a task waits in a queue
    with no worker, so the timeout itself is shown live (break-it), and its setting is checked here."""
    from app.workflows.processing import SCORING

    assert SCORING["schedule_to_close_timeout"] == timedelta(minutes=10)
    assert SCORING["retry_policy"].non_retryable_error_types == [CANNOT_SCORE]


@pytest.mark.parametrize(("signal", "expected"), [("withdraw", "WITHDRAWN"), ("job_closed", "JOB_CLOSED")])
async def test_signals_stop_the_workflow(env, database, signal, expected):
    application_id = await submitted()
    started = workers(env)  # Scoring down: the workflow is waiting when the signal comes
    for worker in started:
        await worker.__aenter__()
    try:
        handle = await env.client.start_workflow(ApplicationWorkflow.run, application_id,
                                                 id=workflows.workflow_id(application_id), task_queue=APPLICATION_TQ)
        await handle.signal(signal)
        assert await handle.result() == expected
    finally:
        for worker in started:
            await worker.__aexit__(None, None, None)
    assert (await fetch("SELECT score_status FROM applications"))[0][0] == "PENDING"  # nothing recorded


async def test_inactive_application_is_not_processed(env, database):
    application_id = await submitted(status="WITHDRAWN")
    score_activity, calls = scoring()
    assert await run(env, application_id, score_activity) == "NOT_ACTIVE" and calls == []


async def test_starting_twice_runs_once(env, database, monkeypatch):
    """The ApplicationSubmitted handler may run twice (redelivery): the fixed workflow id app-{id} with
    REJECT_DUPLICATE makes the second start a no-op, even after the first run finished."""
    async def connect(settings):
        return env.client

    monkeypatch.setattr(workflows, "connect", connect)
    application_id = await submitted()
    score_activity, calls = scoring()
    started = workers(env, score_activity)
    for worker in started:
        await worker.__aenter__()
    try:
        await workflows.start(None, application_id)
        await env.client.get_workflow_handle(workflows.workflow_id(application_id)).result()
        await workflows.start(None, application_id)  # the duplicate: ignored, no error
    finally:
        for worker in started:
            await worker.__aexit__(None, None, None)
    assert calls == [2]  # scored once


# ---------- the record step is idempotent and respects the recruiter ----------


async def test_record_twice_advances_once(database):
    application_id = await submitted()
    result = {"score": 70, "breakdown": {}}
    assert await ActivityEnvironment().run(activities.record_score_and_advance, application_id, result) == "SCREENING"
    assert await ActivityEnvironment().run(activities.record_score_and_advance, application_id, result) == "SCORE_ONLY"
    assert len(await fetch("SELECT id FROM application_stage_history")) == 1
    assert len(await fetch("SELECT id FROM outbox")) == 1


async def test_record_keeps_a_stage_the_recruiter_already_chose(database):
    application_id = await submitted(status="IN_PROGRESS", stage="Interview")
    await ActivityEnvironment().run(activities.record_score_and_advance, application_id, {"score": 60, "breakdown": {}})
    [(status, stage, score)] = await fetch("SELECT status, current_stage, score FROM applications")
    assert (status, stage, float(score)) == ("IN_PROGRESS", "Interview", 60.0)


# ---------- 10.5 job closed + the starter ----------


async def test_job_closed_closes_submitted_applications_frees_quota_and_signals(database, monkeypatch):
    signals = []

    async def fake_signal(settings, application_id, name):
        signals.append((application_id, name))

    monkeypatch.setattr(workflows, "signal", fake_signal)
    open_one = await submitted()
    [(job_id,)] = await fetch("SELECT job_id FROM applications")
    in_progress = await submitted(status="IN_PROGRESS", stage="Interview", job_id=job_id)
    event = new_event("JobClosed", producer="job", payload={"job_id": str(job_id), "recruiter_id": str(uuid.uuid4()), "version": 2})
    async with db.session_scope() as s:
        await handlers.build_handlers(None)["JobClosed"](s, event)

    statuses = dict(await fetch("SELECT id::text, status FROM applications"))
    assert statuses == {open_one: "CLOSED", in_progress: "IN_PROGRESS"}  # the recruiter's work continues
    quotas = sorted(r[0] for r in await fetch("SELECT active_count FROM application_quotas"))
    assert quotas == [0, 1]  # the closed one's slot is free again
    assert (await fetch("SELECT status FROM job_replica"))[0][0] == "CLOSED"
    assert signals == [(open_one, "job_closed")]


async def test_application_submitted_starts_the_workflow(monkeypatch):
    started = []

    async def fake_start(settings, application_id):
        started.append(application_id)

    monkeypatch.setattr(workflows, "start", fake_start)
    event = new_event("ApplicationSubmitted", producer="application", payload={
        "application_id": "a1", "job_id": str(uuid.uuid4()), "candidate_id": str(uuid.uuid4()), "recruiter_id": str(uuid.uuid4())})
    await handlers.build_handlers(None)["ApplicationSubmitted"](None, event)
    assert started == ["a1"]
