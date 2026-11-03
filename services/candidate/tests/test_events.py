"""Step 7.6: UserRegistered -> candidate profile, through the REAL idempotent consumer logic
(EventConsumer.process: processed_events + handler in one transaction) on real Postgres. Kafka itself
is exercised end to end in tests/events at the repo root (make test-events)."""

import uuid

import pytest
from smarthire_common import db, testing
from smarthire_common.events import new_event
from smarthire_common.kafka import EventConsumer
from sqlalchemy import text

from app.events import handlers
from app.repositories import candidates as repo
from tests.conftest import TABLES


@pytest.fixture
async def database(migrated, database_url):
    """The chassis engine for this test only (no app running: init_db is safe here)."""
    await testing.truncate(database_url, TABLES)
    db.init_db(database_url)
    yield
    await db.close_db()


@pytest.fixture
def consumer():
    return EventConsumer(
        name=handlers.CONSUMER_NAME, topics=handlers.TOPICS, handlers=handlers.build_handlers(None),
        bootstrap_servers="unused", schema_registry_url="unused",
    )


def registered(role="CANDIDATE", user_id=None):
    user_id = user_id or uuid.uuid4()
    return new_event("UserRegistered", producer="identity", payload={"user_id": str(user_id), "role": role})


async def count(sql: str) -> int:
    async with db.session_scope() as s:
        return (await s.execute(text(sql))).scalar_one()


async def process(consumer, event) -> bool:
    return await consumer.process(event, consumer.handlers[event.event_type])


async def test_new_candidate_gets_an_empty_profile(database, consumer):
    event = registered()
    assert await process(consumer, event) is True
    async with db.session_scope() as s:
        profile = await repo.get(s, uuid.UUID(event.payload["user_id"]))
    assert profile is not None and profile.full_name == "" and profile.skills == []


async def test_replayed_event_is_skipped(database, consumer):
    event = registered()
    assert await process(consumer, event) is True
    assert await process(consumer, event) is False  # same event_id: duplicate, handler not run
    assert await count("SELECT count(*) FROM candidates") == 1
    assert await count("SELECT count(*) FROM processed_events") == 1


async def test_recruiters_and_admins_get_no_profile(database, consumer):
    for role in ("RECRUITER", "ADMIN"):
        assert await process(consumer, registered(role)) is True  # processed (and recorded)...
    assert await count("SELECT count(*) FROM candidates") == 0  # ...but nothing to do
    assert await count("SELECT count(*) FROM processed_events") == 2


async def test_profile_saved_before_the_event_arrives_is_kept(database, consumer):
    """PUT /candidates/me can win the race against the event: the event must not wipe it."""
    user_id = uuid.uuid4()
    async with db.session_scope() as s:
        await repo.upsert(s, user_id, full_name="Jane Doe", headline=None, skills=["python"],
                          experience_years=None, education=[])
    await process(consumer, registered(user_id=user_id))
    async with db.session_scope() as s:
        profile = await repo.get(s, user_id)
    assert profile.full_name == "Jane Doe" and profile.skills == ["python"]


async def test_a_different_event_for_the_same_user_is_not_a_duplicate(database, consumer):
    """Duplicates are detected by event_id, not by content: two distinct events both run."""
    user_id = uuid.uuid4()
    await process(consumer, registered(user_id=user_id))
    assert await process(consumer, registered(user_id=user_id)) is True
    assert await count("SELECT count(*) FROM candidates") == 1  # and the handler itself is safe to repeat
