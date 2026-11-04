"""Transactional outbox for MongoDB services (Appendix E.11: an `outbox` collection with the same
fields as E.10). Same idea as outbox.py: write the event in the SAME transaction as the change, and a
relay publishes it later.

MongoDB transactions need a replica set: Compose runs Mongo as a single-node replica set (rs0) for
this reason. Unlike Postgres there is no SKIP LOCKED: two relay replicas could send the same event
twice, which the at-least-once design (idempotent consumers) already tolerates."""

from datetime import UTC, datetime
from types import SimpleNamespace

from pymongo import ASCENDING, IndexModel
from pymongo.asynchronous.client_session import AsyncClientSession

from smarthire_common.event_catalog import topic_for
from smarthire_common.events import EventEnvelope
from smarthire_common.mongo import get_db
from smarthire_common.outbox import KafkaSender, OutboxRelay

COLLECTION = "outbox"


async def ensure_outbox_indexes() -> None:
    # The relay's query: unpublished events, oldest first
    await get_db()[COLLECTION].create_indexes([IndexModel([("published_at", ASCENDING), ("created_at", ASCENDING)],
                                                          name="ix_unpublished")])


async def add_outbox_event(event: EventEnvelope, *, key: str, session: AsyncClientSession) -> None:
    """Queue an event inside the caller's transaction (pass the session the change uses)."""
    await get_db()[COLLECTION].insert_one(
        {
            "_id": str(event.event_id),  # unique: the same event can't be queued twice
            "topic": topic_for(event.event_type),
            "message_key": key,
            "event_type": event.event_type,
            "payload": event.model_dump(mode="json"),
            "headers": event.trace_context,
            "created_at": datetime.now(UTC),
            "published_at": None,
        },
        session=session,
    )


class MongoOutboxRelay(OutboxRelay):
    """The Postgres relay's loop with a Mongo query: unpublished docs, oldest first, mark what Kafka
    confirmed. `send(rows)` is the same KafkaSender (rows need .id .topic .message_key ...)."""

    async def publish_batch(self) -> tuple[int, int]:
        import asyncio

        collection = get_db()[COLLECTION]
        docs = await collection.find({"published_at": None}).sort([("created_at", 1), ("_id", 1)]).limit(self.batch_size).to_list()
        if not docs:
            return 0, 0
        rows = [SimpleNamespace(id=d["_id"], topic=d["topic"], message_key=d["message_key"], event_type=d["event_type"],
                                payload=d["payload"], headers=d.get("headers") or {}) for d in docs]
        sent_ids = await asyncio.to_thread(self._send, rows)
        if sent_ids:
            await collection.update_many({"_id": {"$in": sent_ids}}, {"$set": {"published_at": datetime.now(UTC)}})
        return len(sent_ids), len(rows)

    async def measure_backlog(self) -> tuple[int, float]:
        collection = get_db()[COLLECTION]
        count = await collection.count_documents({"published_at": None})
        oldest = await collection.find_one({"published_at": None}, sort=[("created_at", 1)])
        if oldest is None:
            return count, 0.0
        created = oldest["created_at"]
        created = created if created.tzinfo else created.replace(tzinfo=UTC)  # tz-naive if the client isn't tz_aware
        return count, (datetime.now(UTC) - created).total_seconds()


def start_mongo_outbox_relay(settings):
    import asyncio

    sender = KafkaSender(bootstrap_servers=settings.kafka_bootstrap_servers,
                         schema_registry_url=settings.schema_registry_url,
                         client_id=f"{settings.service_name}-outbox-relay")
    return asyncio.create_task(MongoOutboxRelay(sender).run(), name="mongo-outbox-relay")
