"""Transactional outbox (Steps 7.3-7.4).

THE DUAL-WRITE PROBLEM: a use case must change the database AND tell Kafka. Two systems, no shared
transaction: crash between the two and one happens without the other (a user with no
UserRegistered event, or an event about a user whose insert rolled back).

THE FIX: write the event into an `outbox` table in the SAME transaction as the change. Both commit
or neither does. A relay (below) then copies committed rows to Kafka and marks them published."""

import asyncio
import logging
import time
import uuid
from datetime import datetime

from opentelemetry.trace import SpanKind

from sqlalchemy import BigInteger, DateTime, Identity, Index, Text, Uuid, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import column, table

from smarthire_common.db import session_scope
from smarthire_common.event_catalog import topic_for
from smarthire_common.events import EventEnvelope
from smarthire_common.metrics import OUTBOX_BACKLOG, OUTBOX_OLDEST_AGE, OUTBOX_PUBLISHED
from smarthire_common.telemetry import context_from_headers, current_context_headers, get_tracer

log = logging.getLogger(__name__)


class OutboxMixin:
    """Columns of the outbox table (Appendix E.10). Each publishing service declares the model in its
    own app/models (class OutboxEvent(OutboxMixin, Base)), so its migrations own the table."""

    __tablename__ = "outbox"
    # The relay asks "WHERE published_at IS NULL ORDER BY id": this index holds ONLY unpublished
    # rows, so it stays tiny however large the table grows
    __table_args__ = (Index("ix_outbox_unpublished", "id", postgresql_where=text("published_at IS NULL")),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)  # = publish order
    event_id: Mapped[uuid.UUID] = mapped_column(unique=True)
    topic: Mapped[str] = mapped_column(Text)
    message_key: Mapped[str] = mapped_column(Text)  # the aggregate id: same key -> same partition -> in order
    event_type: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict] = mapped_column(JSONB)  # the whole envelope, exactly as it will be sent
    headers: Mapped[dict] = mapped_column(JSONB, server_default=text("'{}'"))  # trace context (Phase 13)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


# Lightweight table: lets the chassis read/write the table without registering a model in the shared
# metadata (which would make every service's migrations expect an outbox table)
_outbox = table(
    "outbox",
    column("id", BigInteger),
    column("event_id", Uuid),
    column("topic", Text),
    column("message_key", Text),
    column("event_type", Text),
    column("payload", JSONB),
    column("headers", JSONB),
    column("created_at", DateTime(timezone=True)),
    column("published_at", DateTime(timezone=True)),
)


async def add_outbox_event(session: AsyncSession, event: EventEnvelope, *, key: str) -> None:
    """Queue an event in the CALLER's transaction: it is published only if the caller commits."""
    await session.execute(
        insert(_outbox).values(
            event_id=event.event_id,
            topic=topic_for(event.event_type),
            message_key=key,
            event_type=event.event_type,
            payload=event.model_dump(mode="json"),
            headers=event.trace_context,
        )
    )


class OutboxRelay:
    """Background task: every `interval` seconds, publish unpublished outbox rows to Kafka.

    AT LEAST ONCE: rows are marked published only after Kafka confirmed them. If the relay crashes
    after Kafka stored a message but before the UPDATE commits, the row is sent again on restart: a
    duplicate, which idempotent consumers skip. The opposite risk (marking first, then failing to
    send) would LOSE the event, which is worse, so we accept duplicates.

    send(rows) -> ids of the rows Kafka confirmed. Default: KafkaSender (tests pass a fake)."""

    def __init__(self, send, *, interval: float = 0.2, batch_size: int = 100, error_backoff: float = 2.0,
                 measure_every: float = 5.0) -> None:
        self._send = send
        self.interval = interval
        self.batch_size = batch_size
        self.error_backoff = error_backoff
        self.measure_every = measure_every  # how often to update the backlog gauges (a COUNT query)

    async def run(self) -> None:
        log.info("outbox relay started")
        measured_at = 0.0
        while True:
            if time.monotonic() - measured_at >= self.measure_every:
                measured_at = time.monotonic()
                try:
                    count, oldest_age = await self.measure_backlog()
                    OUTBOX_BACKLOG.set(count)
                    OUTBOX_OLDEST_AGE.set(oldest_age)  # alert when this grows: Kafka down or relay stuck
                except Exception:
                    log.warning("outbox relay: could not measure the backlog", exc_info=True)
            try:
                sent, total = await self.publish_batch()
            except Exception:
                log.exception("outbox relay: batch failed; retrying")
                await asyncio.sleep(self.error_backoff)
                continue
            if sent < total:
                await asyncio.sleep(self.error_backoff)  # Kafka refused some: don't hammer it
            elif total < self.batch_size:
                await asyncio.sleep(self.interval)  # caught up; a full batch means more is waiting

    async def publish_batch(self) -> tuple[int, int]:
        """One transaction: lock a batch, send it, mark what Kafka confirmed. Returns (sent, total)."""
        async with session_scope() as session:
            rows = (
                await session.execute(
                    select(_outbox.c.id, _outbox.c.topic, _outbox.c.message_key, _outbox.c.event_type,
                           _outbox.c.payload, _outbox.c.headers)
                    .where(_outbox.c.published_at.is_(None))
                    .order_by(_outbox.c.id)  # oldest first: keeps each key's events in order
                    .limit(self.batch_size)
                    # FOR UPDATE: lock these rows until we commit. SKIP LOCKED: rows another relay
                    # (another replica of this service) has locked are skipped, not waited for, so
                    # replicas share the work without sending the same row twice
                    .with_for_update(skip_locked=True)
                )
            ).all()
            if not rows:
                return 0, 0
            sent_ids = await asyncio.to_thread(self._send, rows)  # blocking Kafka calls off the event loop
            if sent_ids:
                await session.execute(
                    update(_outbox).where(_outbox.c.id.in_(sent_ids)).values(published_at=func.now())
                )
        return len(sent_ids), len(rows)

    async def measure_backlog(self) -> tuple[int, float]:
        """(unpublished events, age in seconds of the oldest one; 0 when there are none)."""
        async with session_scope() as session:
            count, age = (await session.execute(
                select(func.count(), func.extract("epoch", func.now() - func.min(_outbox.c.created_at)))
                .where(_outbox.c.published_at.is_(None))
            )).one()
        return count, float(age or 0)


class KafkaSender:
    """Sends outbox rows with the JSON Schema serializer and waits for Kafka's confirmations."""

    def __init__(self, *, bootstrap_servers: str, schema_registry_url: str, client_id: str) -> None:
        from smarthire_common.kafka import EventSerializer, make_producer, make_registry

        self._producer = make_producer(bootstrap_servers, client_id)
        self._serialize = EventSerializer(make_registry(schema_registry_url))

    def __call__(self, rows) -> list[int]:
        confirmed: list[int] = []

        def on_delivery(row_id: int, topic: str):
            def callback(err, _msg) -> None:  # called by flush() below, on this same thread
                if err is None:
                    confirmed.append(row_id)
                    OUTBOX_PUBLISHED.labels(topic).inc()
                else:
                    log.warning("outbox relay: row %s not delivered: %s", row_id, err)

            return callback

        for row in rows:
            try:
                value = self._serialize(row.event_type, row.payload, row.topic)
            except Exception:
                # The event doesn't match its registered schema (or the registry is down). Leave the
                # row unpublished, keep going: other rows shouldn't wait behind a broken one
                log.exception("outbox relay: cannot serialize row %s (%s)", row.id, row.event_type)
                continue
            # A PRODUCER span, CHILD of the code that wrote the event (its context was stored with the row),
            # and THIS span's context goes into the Kafka headers: consumers continue the trace from here
            with get_tracer().start_as_current_span(
                f"publish {row.topic}", context=context_from_headers(row.headers), kind=SpanKind.PRODUCER,
                attributes={"messaging.system": "kafka", "messaging.destination.name": row.topic,
                            "messaging.message.id": str(row.payload.get("event_id")), "event_type": row.event_type},
            ):
                self._producer.produce(
                    row.topic,
                    key=row.message_key.encode(),
                    value=value,
                    headers=list(current_context_headers().items()),
                    on_delivery=on_delivery(row.id, row.topic),
                )
        # Wait for every confirmation (or failure). message.timeout.ms (10 s) < 15 s, so each message
        # is settled one way or the other before we return
        self._producer.flush(15)
        return confirmed


def start_outbox_relay(settings) -> asyncio.Task:
    """Call in the service's lifespan; cancel the returned task on shutdown."""
    sender = KafkaSender(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        schema_registry_url=settings.schema_registry_url,
        client_id=f"{settings.service_name}-outbox-relay",
    )
    return asyncio.create_task(OutboxRelay(sender).run(), name="outbox-relay")
