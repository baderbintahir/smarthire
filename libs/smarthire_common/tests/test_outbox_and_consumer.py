"""Steps 7.3-7.5 against a real Postgres (FOR UPDATE SKIP LOCKED, ON CONFLICT and jsonb are
Postgres features). Kafka is replaced by fakes: these test OUR logic (what gets marked published,
what counts as a duplicate, when a message is retried or dead-lettered), not Kafka itself."""

import asyncio
import uuid

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.orm import DeclarativeBase

from smarthire_common import db, testing
from smarthire_common.events import new_event
from smarthire_common.kafka import EventConsumer, ProcessedEventMixin
from smarthire_common.models import NAMING_CONVENTION
from smarthire_common.outbox import OutboxMixin, OutboxRelay, add_outbox_event


class _Base(DeclarativeBase):  # separate metadata: these test tables stay out of the shared Base
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class _Outbox(OutboxMixin, _Base):
    pass


class _Processed(ProcessedEventMixin, _Base):
    pass


@pytest.fixture(scope="module")
def database_url():
    with testing.service_postgres("chassis") as url:
        yield url


@pytest.fixture
async def database(database_url):
    db.init_db(database_url)
    async with db.get_engine().begin() as conn:
        await conn.run_sync(_Base.metadata.drop_all)
        await conn.run_sync(_Base.metadata.create_all)
    yield
    await db.close_db()


def user_registered(user_id=None):
    return new_event("UserRegistered", producer="test", payload={"user_id": str(user_id or uuid.uuid4()), "role": "CANDIDATE"})


async def scalar(sql: str, **params):
    async with db.session_scope() as s:
        return (await s.execute(text(sql), params)).scalar_one()


# ---------- 7.3 outbox writer ----------


async def test_outbox_row_commits_with_the_callers_transaction(database):
    event = user_registered()
    async with db.session_scope() as s:
        await add_outbox_event(s, event, key="user-1")
    row = await scalar("SELECT payload FROM outbox")
    assert row["event_id"] == str(event.event_id) and row["payload"]["role"] == "CANDIDATE"  # the whole envelope
    assert await scalar("SELECT topic || '/' || message_key FROM outbox") == "identity.users/user-1"


async def test_outbox_row_rolls_back_with_the_callers_transaction(database):
    with pytest.raises(RuntimeError):
        async with db.session_scope() as s:
            await add_outbox_event(s, user_registered(), key="k")
            raise RuntimeError("the use case failed after writing its event")
    assert await scalar("SELECT count(*) FROM outbox") == 0  # no event about something that didn't happen


# ---------- 7.4 relay ----------


class FakeKafka:
    """Stands in for KafkaSender: records what was sent, can refuse some rows."""

    def __init__(self, refuse_keys=()):
        self.sent: list[str] = []
        self.refuse_keys = set(refuse_keys)

    def __call__(self, rows):
        ok = [r for r in rows if r.message_key not in self.refuse_keys]
        self.sent += [r.message_key for r in ok]
        return [r.id for r in ok]


async def add_events(keys):
    async with db.session_scope() as s:
        for key in keys:
            await add_outbox_event(s, user_registered(), key=key)


async def test_relay_publishes_in_order_and_marks_rows(database):
    await add_events(["a", "b", "c"])
    kafka = FakeKafka()
    assert await OutboxRelay(kafka).publish_batch() == (3, 3)
    assert kafka.sent == ["a", "b", "c"]  # oldest first
    assert await scalar("SELECT count(*) FROM outbox WHERE published_at IS NULL") == 0
    assert await OutboxRelay(kafka).publish_batch() == (0, 0)  # nothing left: nothing re-sent


async def test_unconfirmed_rows_stay_unpublished_for_the_next_round(database):
    await add_events(["a", "b"])
    assert await OutboxRelay(FakeKafka(refuse_keys={"b"})).publish_batch() == (1, 2)
    kafka = FakeKafka()
    await OutboxRelay(kafka).publish_batch()
    assert kafka.sent == ["b"]  # retried later, not lost


async def test_kafka_down_marks_nothing(database):
    await add_events(["a"])

    def down(rows):
        raise ConnectionError("broker unreachable")

    with pytest.raises(ConnectionError):
        await OutboxRelay(down).publish_batch()
    assert await scalar("SELECT count(*) FROM outbox WHERE published_at IS NULL") == 1  # waits in the outbox


async def test_two_relays_never_send_the_same_row(database):
    """SKIP LOCKED: while relay A holds its batch, relay B skips those rows instead of waiting."""
    await add_events([f"k{i}" for i in range(10)])
    a_has_rows, release_a = asyncio.Event(), asyncio.Event()
    sent_by: dict[str, list[str]] = {"a": [], "b": []}

    def slow_a(rows):  # runs in a worker thread, so B can run meanwhile
        sent_by["a"] += [r.message_key for r in rows]
        loop.call_soon_threadsafe(a_has_rows.set)
        asyncio.run_coroutine_threadsafe(release_a.wait(), loop).result()
        return [r.id for r in rows]

    def fast_b(rows):
        sent_by["b"] += [r.message_key for r in rows]
        return [r.id for r in rows]

    loop = asyncio.get_running_loop()
    task_a = asyncio.create_task(OutboxRelay(slow_a, batch_size=4).publish_batch())
    await a_has_rows.wait()
    await OutboxRelay(fast_b, batch_size=100).publish_batch()
    release_a.set()
    await task_a
    assert len(sent_by["a"]) == 4 and len(sent_by["b"]) == 6
    assert not set(sent_by["a"]) & set(sent_by["b"])


# ---------- 7.5 idempotent consumer ----------


def make_consumer(handlers, **kwargs) -> EventConsumer:
    consumer = EventConsumer(
        name="test-consumer", topics=["identity.users"], handlers=handlers,
        bootstrap_servers="unused", schema_registry_url="unused", backoff_seconds=0, **kwargs,
    )
    consumer.dead_lettered = []

    async def fake_dead_letter(msg, error):
        consumer.dead_lettered.append((msg.value(), str(error)))

    consumer.dead_letter = fake_dead_letter
    return consumer


class FakeMessage:
    def __init__(self, value):
        self._value = value

    def value(self):
        return self._value

    def topic(self):
        return "identity.users"


async def test_same_event_twice_runs_the_handler_once(database):
    calls = []

    async def handler(session, event):
        calls.append(event.event_id)
        await session.execute(text("SELECT 1"))  # the handler gets the SAME transaction

    consumer, event = make_consumer({}), user_registered()
    assert await consumer.process(event, handler) is True
    assert await consumer.process(event, handler) is False  # redelivered: skipped
    assert calls == [event.event_id]
    assert await scalar("SELECT count(*) FROM processed_events") == 1


async def test_failed_handler_rolls_back_the_claim_so_a_retry_can_run(database):
    attempts = []

    async def flaky(session, event):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("database hiccup")

    consumer, event = make_consumer({}), user_registered()
    with pytest.raises(RuntimeError):
        await consumer.process(event, flaky)
    assert await scalar("SELECT count(*) FROM processed_events") == 0  # not marked as processed
    assert await consumer.process(event, flaky) is True


async def test_handler_retries_then_dead_letters(database):
    attempts = []

    async def always_fails(session, event):
        attempts.append(1)
        raise RuntimeError("bug in the handler")

    consumer = make_consumer({"UserRegistered": always_fails}, max_attempts=3)
    event = user_registered()
    consumer._deserialize = lambda msg: event.model_dump(mode="json")
    await consumer.handle_message(FakeMessage(b"..."))  # returns: the caller may commit the offset
    assert len(attempts) == 3
    assert len(consumer.dead_lettered) == 1 and "bug in the handler" in consumer.dead_lettered[0][1]


async def test_poison_message_goes_straight_to_the_dlq(database):
    consumer = make_consumer({"UserRegistered": None})

    def broken(msg):
        raise ValueError("not JSON")

    consumer._deserialize = broken
    await consumer.handle_message(FakeMessage(b"\x00garbage"))
    assert consumer.dead_lettered == [(b"\x00garbage", "not JSON")]


async def test_event_types_without_a_handler_are_skipped(database):
    consumer = make_consumer({})  # interested in nothing
    event = user_registered()
    consumer._deserialize = lambda msg: event.model_dump(mode="json")
    await consumer.handle_message(FakeMessage(b"..."))
    assert consumer.dead_lettered == [] and await scalar("SELECT count(*) FROM processed_events") == 0


async def test_registry_outage_is_retried_not_dead_lettered(database):
    consumer = make_consumer({"UserRegistered": None})

    def registry_down(msg):
        raise ConnectionError("schema registry unreachable")

    consumer._deserialize = registry_down
    with pytest.raises(ConnectionError):  # run() then seeks back and tries again later
        await consumer.handle_message(FakeMessage(b"..."))
    assert consumer.dead_lettered == []
