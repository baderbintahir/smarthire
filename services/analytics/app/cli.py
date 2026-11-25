"""Admin commands.

    docker compose exec analytics python -m app.cli rebuild
        Rebuild the read model from Analytics' own `facts` table (design v2.0): empty the projections and
        re-apply every fact in time order, in one transaction. Analytics can keep running meanwhile: new
        facts wait for the rebuild to commit. The numbers come back identical, the proof that the
        projections are only a VIEW of the facts.

    docker compose exec analytics python -m app.cli backfill-facts
        One-off (Step T4, while Kafka still exists): copy every past event from the Kafka topics into
        `facts`, keyed by event id, WITHOUT applying them (the projections already count them). After it, a
        rebuild from facts gives today's numbers, and Kafka's history is no longer needed."""

import argparse
import asyncio

from smarthire_common.db import close_db, init_db, session_scope
from smarthire_common.events import EventEnvelope
from smarthire_common.kafka import EventConsumer
from smarthire_common.logging import setup_logging
from smarthire_common.redis import close_redis, get_redis, init_redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.events import handlers
from app.repositories import facts as facts_repo
from app.services import facts
from app.services.metrics import CACHE_KEY

BACKFILL_GROUP = "analytics-backfill"  # its own consumer group: it reads everything from offset 0


async def rebuild(settings: Settings) -> int:
    init_db(settings.database_url)
    init_redis(settings.redis_url)
    try:
        async with session_scope() as session:
            count = await facts.rebuild(session)
        await get_redis().delete(CACHE_KEY)  # don't serve numbers cached before the rebuild
        return count
    finally:
        await close_redis()
        await close_db()


async def _store_only(session: AsyncSession, event: EventEnvelope) -> None:
    await facts_repo.insert_fact(session, fact_id=str(event.event_id), kind=event.event_type,
                                 occurred_at=event.occurred_at, data=facts.slim(event.event_type, event.payload))


async def backfill(settings: Settings) -> int:
    init_db(settings.database_url)
    try:
        consumer = EventConsumer(name=BACKFILL_GROUP, topics=handlers.TOPICS,
                                 handlers={kind: _store_only for kind in facts.APPLIERS},
                                 bootstrap_servers=settings.kafka_bootstrap_servers,
                                 schema_registry_url=settings.schema_registry_url)
        return await consumer.replay_from_beginning()
    finally:
        await close_db()


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m app.cli")
    parser.add_argument("command", choices=["rebuild", "backfill-facts"])
    command = parser.parse_args().command
    settings = Settings()
    setup_logging(f"{settings.service_name}-cli", settings.log_level)
    if command == "rebuild":
        print(f"rebuilt the analytics read model from {asyncio.run(rebuild(settings))} facts")
    else:
        print(f"read {asyncio.run(backfill(settings))} events from Kafka into facts")


if __name__ == "__main__":
    main()
