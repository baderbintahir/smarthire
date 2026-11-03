"""Kafka clients and the idempotent consumer runner (Steps 7.4-7.5).

Delivery is AT LEAST ONCE: a consumer can receive the same event twice (e.g. it crashed after
processing but before committing its offset). So every consumer is idempotent: it records the
event_id in `processed_events` in the SAME transaction as its own changes. A redelivered event
finds its id already there and is skipped. Effects happen once, even though delivery may not."""

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, Message, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient, topic_record_subject_name_strategy
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError
from pydantic import ValidationError
from sqlalchemy import DateTime, Text, Uuid, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import column, table

from smarthire_common.db import session_scope
from smarthire_common.event_catalog import schema_str
from smarthire_common.events import EventEnvelope

log = logging.getLogger(__name__)

# ---------------------------------------------------------------- clients


def make_producer(bootstrap_servers: str, client_id: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            # Retries can't duplicate or reorder messages within a partition (implies acks=all:
            # the broker confirms only once the message is safely stored)
            "enable.idempotence": True,
            # Give up on a message after 10 s (default: 5 min), so a batch is settled one way or the
            # other while the relay still holds its rows (see outbox.OutboxRelay)
            "message.timeout.ms": 10_000,
            "logger": logging.getLogger("librdkafka"),  # librdkafka's own logs go through our JSON logging
        }
    )


def make_registry(url: str) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": url})


class EventSerializer:
    """Turns an envelope (dict) into Kafka bytes, checked against the REGISTERED schema of its
    event type. One JSONSerializer per event type, created on first use."""

    def __init__(self, registry: SchemaRegistryClient) -> None:
        self._registry = registry
        self._by_type: dict[str, JSONSerializer] = {}

    def __call__(self, event_type: str, envelope: dict, topic: str) -> bytes:
        serializer = self._by_type.get(event_type)
        if serializer is None:
            serializer = self._by_type[event_type] = JSONSerializer(
                schema_str(event_type),
                self._registry,
                conf={
                    # Producers never register schemas: the schema-init container does. An unknown or
                    # changed schema then fails loudly here instead of silently becoming a new version
                    "auto.register.schemas": False,
                    "subject.name.strategy": topic_record_subject_name_strategy,  # <topic>-<EventType>
                },
            )
        # Validates against the schema, then writes: magic byte + schema id + JSON
        return serializer(envelope, SerializationContext(topic, MessageField.VALUE))


# ---------------------------------------------------------------- processed_events (E.10)


class ProcessedEventMixin:
    """Columns of the processed_events table. Each consuming service declares the model in its own
    app/models (class ProcessedEvent(ProcessedEventMixin, Base)), so its migrations own the table."""

    __tablename__ = "processed_events"

    # Composite primary key (consumer_name, event_id): inserting a duplicate is how we detect one
    consumer_name: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# Lightweight table: lets this module write the table without registering a model in the shared
# metadata (which would make every service's migrations expect a processed_events table)
_processed = table("processed_events", column("consumer_name", Text), column("event_id", Uuid))

# ---------------------------------------------------------------- consumer runner


class RetryLater(Exception):
    """Raise from a handler when a DEPENDENCY is down (e.g. Temporal): the event is fine, the time is
    wrong. It skips the retries-then-DLQ path: the consumer goes back to the message and tries again
    in a few seconds, for as long as it takes."""


Handler = Callable[[AsyncSession, EventEnvelope], Awaitable[None]]


class EventConsumer:
    """Runs one consumer group: poll -> process idempotently -> commit the offset.

    handlers maps event_type -> async handler(session, envelope). Event types without a handler are
    skipped (topics are shared: Candidate reads identity.users but may not care about every type).

    Order of commits matters: the DB transaction commits FIRST, the Kafka offset SECOND. A crash in
    between means the event comes again, and processed_events makes the repeat a no-op. The other
    order could lose the event: offset saved ("done"), DB change never committed."""

    def __init__(
        self,
        *,
        name: str,
        topics: list[str],
        handlers: dict[str, Handler],
        bootstrap_servers: str,
        schema_registry_url: str,
        max_attempts: int = 3,
        backoff_seconds: float = 0.5,
    ) -> None:
        self.name = name  # also the consumer group id and the consumer_name in processed_events
        self.topics = topics
        self.handlers = handlers
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self._bootstrap_servers = bootstrap_servers
        self._registry_url = schema_registry_url
        self._deserializer: JSONDeserializer | None = None
        self._dlq_producer: Producer | None = None

    # ---- the loop (needs Kafka) ----

    async def run(self) -> None:
        consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": self.name,  # all replicas of this service share the partitions
                "enable.auto.commit": False,  # we commit offsets ourselves, after the DB commit
                "auto.offset.reset": "earliest",  # a brand-new group starts from the oldest event
                "logger": logging.getLogger("librdkafka"),
            }
        )
        consumer.subscribe(self.topics)
        log.info("consumer %s started on %s", self.name, self.topics)
        try:
            while True:
                # poll() blocks for up to 1 s: run it in a thread so the event loop stays free
                msg = await asyncio.to_thread(consumer.poll, 1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("consumer %s: %s", self.name, msg.error())
                    continue
                try:
                    await self.handle_message(msg)
                except Exception:
                    # Transient trouble (DB or registry down, DLQ unreachable): don't commit, go back
                    # to this message and try it again shortly
                    log.exception("consumer %s: will retry %s[%s]@%s", self.name, msg.topic(), msg.partition(), msg.offset())
                    await asyncio.to_thread(consumer.seek, TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    await asyncio.sleep(5)
                    continue
                await asyncio.to_thread(consumer.commit, message=msg, asynchronous=False)
        finally:
            await asyncio.to_thread(consumer.close)  # leave the group cleanly: partitions move on at once

    async def replay_from_beginning(self) -> int:
        """Re-read the topics from their FIRST message up to where they end right now, process every
        message like run() does, commit offsets, then stop. Used to rebuild a read model (Analytics,
        Step 12.4). The service's own consumer must be stopped meanwhile: the group would otherwise
        split the partitions between the two. Returns how many messages were read."""
        consumer = Consumer({"bootstrap.servers": self._bootstrap_servers, "group.id": self.name,
                             "enable.auto.commit": False, "logger": logging.getLogger("librdkafka")})

        def from_the_start(c, partitions):
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING  # ignore the group's saved bookmark
            c.assign(partitions)

        consumer.subscribe(self.topics, on_assign=from_the_start)
        ends: dict[tuple[str, int], int] = {}  # where each partition ends NOW (later messages: run()'s job)
        read = 0
        try:
            while True:
                msg = await asyncio.to_thread(consumer.poll, 1.0)
                if not ends and consumer.assignment():
                    for tp in consumer.assignment():
                        low, high = await asyncio.to_thread(consumer.get_watermark_offsets, tp, 10)
                        ends[(tp.topic, tp.partition)] = high if high > low else -1  # -1: empty, nothing to read
                if msg is not None and not msg.error():
                    await self.handle_message(msg)
                    await asyncio.to_thread(consumer.commit, message=msg, asynchronous=False)
                    read += 1
                    if ends and msg.offset() + 1 >= ends.get((msg.topic(), msg.partition()), 0):
                        ends[(msg.topic(), msg.partition())] = -1  # this partition is done
                if ends and all(end == -1 for end in ends.values()):
                    return read
        finally:
            await asyncio.to_thread(consumer.close)

    # ---- one message (testable without Kafka) ----

    async def handle_message(self, msg: Message) -> None:
        """Returns normally when the message is DONE (processed, skipped, or dead-lettered): the
        caller then commits its offset. Raises when it should be retried later."""
        try:
            envelope = EventEnvelope.model_validate(self._deserialize(msg))
        except (SerializationError, ValidationError, ValueError) as exc:
            # Unreadable (not JSON, not our format) or invalid: retrying can't help ("poison
            # message"). Park it and move on. A registry that is DOWN raises other errors: retried
            await self.dead_letter(msg, exc)
            return
        handler = self.handlers.get(envelope.event_type)
        if handler is None:
            return
        for attempt in range(1, self.max_attempts + 1):
            try:
                await self.process(envelope, handler)
                return
            except RetryLater:
                raise  # not the event's fault: run() seeks back and retries later, never DLQ
            except Exception as exc:
                if attempt == self.max_attempts:
                    log.exception("consumer %s: %s %s failed %d times", self.name, envelope.event_type, envelope.event_id, attempt)
                    await self.dead_letter(msg, exc)
                    return
                await asyncio.sleep(self.backoff_seconds * 2 ** (attempt - 1))  # 0.5 s, 1 s, ...

    async def process(self, envelope: EventEnvelope, handler: Handler) -> bool:
        """One transaction: claim the event_id, then run the handler. Returns False for a duplicate.
        If the handler fails, the claim rolls back with it, so a retry can claim it again."""
        async with session_scope() as session:
            claimed = await session.execute(
                insert(_processed)
                .values(consumer_name=self.name, event_id=envelope.event_id)
                .on_conflict_do_nothing()  # already there = processed before
                .returning(_processed.c.event_id)
            )
            if claimed.first() is None:
                log.info("consumer %s: skipped duplicate %s %s", self.name, envelope.event_type, envelope.event_id)
                return False
            await handler(session, envelope)
        return True

    async def dead_letter(self, msg: Message, error: Exception) -> None:
        """Copy the message, unchanged, to <topic>.dlq with the error in its headers, for a human to
        inspect and replay. Raises if the DLQ write fails (then the message is retried, not lost)."""
        if self._dlq_producer is None:
            self._dlq_producer = make_producer(self._bootstrap_servers, f"{self.name}-dlq")
        headers = list(msg.headers() or []) + [
            ("x-error", f"{type(error).__name__}: {error}"[:1000]),
            ("x-consumer", self.name),
            ("x-original-topic", msg.topic()),
            ("x-original-partition", str(msg.partition())),
            ("x-original-offset", str(msg.offset())),
        ]
        await asyncio.to_thread(self._produce_and_wait, f"{msg.topic()}.dlq", msg.key(), msg.value(), headers)
        log.error("consumer %s: sent %s[%s]@%s to the DLQ: %s", self.name, msg.topic(), msg.partition(), msg.offset(), error)

    def _produce_and_wait(self, topic: str, key, value, headers) -> None:
        result: list = []
        self._dlq_producer.produce(topic, key=key, value=value, headers=headers, on_delivery=lambda err, _m: result.append(err))
        self._dlq_producer.flush(15)
        if not result or result[0] is not None:
            raise RuntimeError(f"could not write to {topic}: {result[0] if result else 'timeout'}")

    def _deserialize(self, msg: Message) -> dict:
        if self._deserializer is None:
            # schema_str=None: use the WRITER's schema (its id is inside the message) to validate
            self._deserializer = JSONDeserializer(None, schema_registry_client=make_registry(self._registry_url))
        return self._deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))


def start_consumer(settings, *, name: str, topics: list[str], handlers: dict[str, Handler]) -> asyncio.Task:
    """Call in the service's lifespan; cancel the returned task on shutdown."""
    consumer = EventConsumer(
        name=name,
        topics=topics,
        handlers=handlers,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        schema_registry_url=settings.schema_registry_url,
    )
    return asyncio.create_task(consumer.run(), name=f"consumer-{name}")
