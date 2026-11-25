"""Step 8.2 (design v2.1): job_replica kept current by the orchestrator's sync_job_replica and
close_job_applications steps, on Postgres. Late or repeated steps never roll it back."""

import uuid

import pytest
from smarthire_common import db, testing

from app.repositories import job_replica
from app.services import replica
from tests.conftest import TABLES


@pytest.fixture
async def database(migrated, database_url):
    await testing.truncate(database_url, TABLES)
    db.init_db(database_url)  # safe here: no app is running in these tests
    yield
    await db.close_db()


JOB_ID = str(uuid.uuid4())


def snapshot(version=1, **changes):
    return {"job_id": JOB_ID, "recruiter_id": str(uuid.uuid4()), "title": "Backend Engineer", "status": "DRAFT",
            "stages": ["Screening", "Interview"], "eligibility_rules": {}, "application_limit": None,
            "deadline": None, "version": version, **changes}


async def sync(snap) -> bool:
    async with db.session_scope() as s:
        return await replica.sync(s, snap)


async def close(version) -> list:
    async with db.session_scope() as s:
        return await replica.close_job(s, uuid.UUID(JOB_ID), version)


async def stored():
    async with db.session_scope() as s:
        return await job_replica.get(s, uuid.UUID(JOB_ID))


async def test_first_snapshot_creates_the_replica(database):
    assert await sync(snapshot(version=1, deadline="2026-12-01T00:00:00+00:00")) is True
    job = await stored()
    assert job.title == "Backend Engineer" and job.source_version == 1 and job.deadline.year == 2026


async def test_newer_snapshot_updates_it(database):
    await sync(snapshot(version=1))
    await sync(snapshot(version=3, status="READY", title="Senior Backend Engineer"))
    job = await stored()
    assert (job.status, job.title, job.source_version) == ("READY", "Senior Backend Engineer", 3)


async def test_older_or_repeated_snapshot_is_ignored(database):
    await sync(snapshot(version=3, status="READY"))
    assert await sync(snapshot(version=2, status="DRAFT", title="Old title")) is False  # late
    assert await sync(snapshot(version=3, status="READY")) is False  # a retried step
    job = await stored()
    assert (job.status, job.title, job.source_version) == ("READY", "Backend Engineer", 3)


async def test_close_applies_only_if_newer_and_once(database):
    await sync(snapshot(version=3, status="READY"))
    assert await close(version=2) == [] and (await stored()).status == "READY"  # stale
    await close(version=4)
    assert (await stored()).status == "CLOSED"
    assert await close(version=4) == []  # a retried step: nothing more to do


async def test_close_for_an_unknown_job_changes_nothing(database):
    assert await close(version=1) == []
    assert await stored() is None
