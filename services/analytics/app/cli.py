"""Admin commands (Step 12.4). Rebuild the whole read model from the events:

    docker compose stop analytics
    docker compose run --rm analytics python -m app.cli rebuild
    docker compose start analytics

The projections are disposable: empty them, re-read every topic from its first message, and the
numbers come back identical. That is the proof that Analytics owns no data, only a VIEW of events."""

import argparse
import asyncio

from confluent_kafka.admin import AdminClient
from smarthire_common.db import close_db, init_db, session_scope
from smarthire_common.kafka import EventConsumer
from smarthire_common.logging import setup_logging
from smarthire_common.redis import close_redis, get_redis, init_redis
from sqlalchemy import text

from app.config import Settings
from app.events import handlers
from app.models.projections import PROJECTION_TABLES
from app.services.metrics import CACHE_KEY


def _group_is_idle(settings: Settings) -> bool:
    """The live consumer must be stopped: it would share the partitions with the rebuild."""
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    description = admin.describe_consumer_groups([handlers.CONSUMER_NAME])[handlers.CONSUMER_NAME].result(timeout=10)
    return not description.members


async def _wait_until_idle(settings: Settings, timeout: float = 60.0) -> bool:
    """A consumer that stopped cleanly leaves the group at once; one that was killed lingers until Kafka's
    session timeout (45 s) expires. So wait a little before giving up."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not await asyncio.to_thread(_group_is_idle, settings):
        if asyncio.get_running_loop().time() > deadline:
            return False
        await asyncio.sleep(3)
    return True


async def rebuild(settings: Settings) -> int:
    if not await _wait_until_idle(settings):
        raise SystemExit("The analytics consumer is running: `docker compose stop analytics` first")
    init_db(settings.database_url)
    init_redis(settings.redis_url)
    try:
        # Empty the read model and forget which events were processed (they will all be processed again)
        async with session_scope() as session:
            await session.execute(text(f"TRUNCATE {', '.join(PROJECTION_TABLES + ['processed_events'])}"))
        consumer = EventConsumer(name=handlers.CONSUMER_NAME, topics=handlers.TOPICS, handlers=handlers.HANDLERS,
                                 bootstrap_servers=settings.kafka_bootstrap_servers,
                                 schema_registry_url=settings.schema_registry_url)
        replayed = await consumer.replay_from_beginning()
        await get_redis().delete(CACHE_KEY)  # don't serve numbers cached before the rebuild
        return replayed
    finally:
        await close_redis()
        await close_db()


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m app.cli")
    parser.add_argument("command", choices=["rebuild"])
    parser.parse_args()
    settings = Settings()
    setup_logging(f"{settings.service_name}-cli", settings.log_level)
    count = asyncio.run(rebuild(settings))
    print(f"rebuilt the analytics read model from {count} events")


if __name__ == "__main__":
    main()
