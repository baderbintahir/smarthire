"""Step 8.2: job_replica from Job's events, through the real idempotent consumer logic on Postgres."""

import uuid

import pytest
from smarthire_common import db, testing
from smarthire_common.events import new_event
from smarthire_common.kafka import EventConsumer

from app.events import handlers
from app.repositories import job_replica
from tests.conftest import TABLES


@pytest.fixture
async def database(migrated, database_url):
    await testing.truncate(database_url, TABLES)
    db.init_db(database_url)  # safe here: no app is running in these tests
    yield
    await db.close_db()


@pytest.fixture
def consumer():
    return EventConsumer(name=handlers.CONSUMER_NAME, topics=handlers.TOPICS, handlers=handlers.HANDLERS,
                         bootstrap_servers="unused", schema_registry_url="unused")


JOB_ID = str(uuid.uuid4())


def snapshot(event_type="JobUpdated", version=1, **changes):
    payload = {"job_id": JOB_ID, "recruiter_id": str(uuid.uuid4()), "title": "Backend Engineer", "status": "DRAFT",
               "stages": ["Screening", "Interview"], "eligibility_rules": {}, "application_limit": None,
               "deadline": None, "version": version, **changes}
    return new_event(event_type, producer="job", payload=payload)


def closed(version):
    return new_event("JobClosed", producer="job", payload={"job_id": JOB_ID, "recruiter_id": str(uuid.uuid4()), "version": version})


async def deliver(consumer, event):
    return await consumer.process(event, handlers.HANDLERS[event.event_type])


async def replica():
    async with db.session_scope() as s:
        return await job_replica.get(s, uuid.UUID(JOB_ID))


async def test_first_event_creates_the_replica(database, consumer):
    await deliver(consumer, snapshot(version=1, deadline="2026-12-01T00:00:00+00:00"))
    job = await replica()
    assert job.title == "Backend Engineer" and job.source_version == 1 and job.deadline.year == 2026


async def test_newer_event_updates_it(database, consumer):
    await deliver(consumer, snapshot(version=1))
    await deliver(consumer, snapshot("JobReady", version=3, status="READY", title="Senior Backend Engineer"))
    job = await replica()
    assert (job.status, job.title, job.source_version) == ("READY", "Senior Backend Engineer", 3)


async def test_older_event_arriving_late_is_ignored(database, consumer):
    await deliver(consumer, snapshot("JobReady", version=3, status="READY"))
    await deliver(consumer, snapshot(version=2, status="DRAFT", title="Old title"))  # late, out of order
    job = await replica()
    assert (job.status, job.title, job.source_version) == ("READY", "Backend Engineer", 3)


async def test_job_closed_applies_only_if_newer(database, consumer):
    await deliver(consumer, snapshot("JobReady", version=3, status="READY"))
    await deliver(consumer, closed(version=2))  # stale
    assert (await replica()).status == "READY"
    await deliver(consumer, closed(version=4))
    assert (await replica()).status == "CLOSED"


async def test_job_closed_for_an_unknown_job_changes_nothing(database, consumer):
    assert await deliver(consumer, closed(version=1)) is True  # processed, nothing to update
    assert await replica() is None


async def test_redelivered_event_is_skipped(database, consumer):
    event = snapshot(version=1)
    assert await deliver(consumer, event) is True
    assert await deliver(consumer, event) is False
