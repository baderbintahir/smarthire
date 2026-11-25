"""Design v2.1: Application's workflow steps on Postgres. The workflows themselves (ApplicationWorkflow,
ApplicationActionWorkflow) live in the orchestrator and are tested there; here: each step does its
change once, however often Temporal runs it."""

import uuid

import pytest
from smarthire_common import db, testing
from sqlalchemy import text

from app.models import Application, ApplicationQuota, JobReplica
from app.services import replica, steps
from tests.conftest import TABLES


@pytest.fixture
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


async def record(application_id: str, score) -> dict:
    async with db.session_scope() as s:
        return await steps.record_score_and_advance(s, uuid.UUID(application_id), score)


async def test_scored_application_moves_to_screening_once(database):
    application_id = await submitted()
    assert await record(application_id, 70) == {"advanced": True}
    assert await record(application_id, 70) == {"advanced": False}  # a retried step: no second history row
    [(status, stage, score, score_status)] = await fetch(
        "SELECT status, current_stage, score, score_status FROM applications")
    assert (status, stage, float(score), score_status) == ("SCREENING", "SCREENING", 70.0, "SCORED")
    assert len(await fetch("SELECT id FROM application_stage_history")) == 1


async def test_unscorable_application_is_pending_manual_but_still_screened(database):
    application_id = await submitted()
    assert await record(application_id, None) == {"advanced": True}
    assert (await fetch("SELECT status, score_status FROM applications"))[0] == ("SCREENING", "PENDING_MANUAL")


async def test_record_keeps_a_stage_the_recruiter_already_chose(database):
    application_id = await submitted(status="IN_PROGRESS", stage="Interview")
    assert await record(application_id, 60) == {"advanced": False}
    [(status, stage, score)] = await fetch("SELECT status, current_stage, score FROM applications")
    assert (status, stage, float(score)) == ("IN_PROGRESS", "Interview", 60.0)


async def test_inactive_application_is_left_alone(database):
    application_id = await submitted(status="WITHDRAWN", stage="SUBMITTED")
    assert await record(application_id, 90) == {"advanced": False}
    assert (await fetch("SELECT score FROM applications"))[0][0] is None


async def test_job_closed_closes_submitted_applications_and_frees_quota(database):
    """close_job_applications step: returns the closed ones, whose workflows the orchestrator stops."""
    open_one = await submitted()
    [(job_id,)] = await fetch("SELECT job_id FROM applications")
    in_progress = await submitted(status="IN_PROGRESS", stage="Interview", job_id=job_id)
    async with db.session_scope() as s:
        closed = await replica.close_job(s, job_id, 2)

    statuses = dict(await fetch("SELECT id::text, status FROM applications"))
    assert statuses == {open_one: "CLOSED", in_progress: "IN_PROGRESS"}  # the recruiter's work continues
    quotas = sorted(r[0] for r in await fetch("SELECT active_count FROM application_quotas"))
    assert quotas == [0, 1]  # the closed one's slot is free again
    assert (await fetch("SELECT status FROM job_replica"))[0][0] == "CLOSED"
    assert [c["application_id"] for c in closed] == [open_one]
