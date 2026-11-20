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


def registered(role="CANDIDATE", user_id=None, roles=None):
    """A v2 event (roles array). roles=False builds a v1 event (single role only), as old replays are."""
    user_id = user_id or uuid.uuid4()
    payload = {"user_id": str(user_id), "role": role}
    if roles is not False:
        payload["roles"] = roles or [role]
    return new_event("UserRegistered", producer="identity", payload=payload)


def granted(user_id, role="CANDIDATE"):
    payload = {"user_id": str(user_id), "role": role, "granted_by": None}
    return new_event("RoleGranted", producer="identity", payload=payload)


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
    assert profile is not None and profile.skills == [] and profile.headline is None


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
        await repo.upsert(s, user_id, headline="Jane's headline", skills=["python"],
                          experience_years=None, education=[])
    await process(consumer, registered(user_id=user_id))
    async with db.session_scope() as s:
        profile = await repo.get(s, user_id)
    assert profile.headline == "Jane's headline" and profile.skills == ["python"]


async def test_a_different_event_for_the_same_user_is_not_a_duplicate(database, consumer):
    """Duplicates are detected by event_id, not by content: two distinct events both run."""
    user_id = uuid.uuid4()
    await process(consumer, registered(user_id=user_id))
    assert await process(consumer, registered(user_id=user_id)) is True
    assert await count("SELECT count(*) FROM candidates") == 1  # and the handler itself is safe to repeat


# ---------- design v1.1: several roles, RoleGranted ----------


async def test_registration_with_candidate_among_several_roles_creates_a_profile(database, consumer):
    event = registered("RECRUITER", roles=["RECRUITER", "CANDIDATE"])  # `role` = first role, not CANDIDATE
    await process(consumer, event)
    assert await count("SELECT count(*) FROM candidates") == 1


async def test_v1_event_without_roles_still_works(database, consumer):
    await process(consumer, registered("CANDIDATE", roles=False))  # replay of an event from before v1.1
    await process(consumer, registered("RECRUITER", roles=False))
    assert await count("SELECT count(*) FROM candidates") == 1


async def test_role_granted_candidate_creates_a_profile_once(database, consumer):
    user_id = uuid.uuid4()
    await process(consumer, registered("RECRUITER", user_id=user_id))
    assert await count("SELECT count(*) FROM candidates") == 0  # a recruiter has no candidate profile...
    event = granted(user_id)
    assert await process(consumer, event) is True
    assert await count("SELECT count(*) FROM candidates") == 1  # ...until they take the CANDIDATE role
    assert await process(consumer, event) is False  # replay: skipped by processed_events
    assert await process(consumer, granted(user_id)) is True  # a second grant event: handler is a no-op
    assert await count("SELECT count(*) FROM candidates") == 1


async def test_other_role_grants_create_nothing(database, consumer):
    await process(consumer, granted(uuid.uuid4(), "RECRUITER"))
    assert await count("SELECT count(*) FROM candidates") == 0
