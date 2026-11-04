"""Data retention: scheduled clean-up of rows that are no longer needed (Step 14.3b, Appendix E.14).

Without it, `outbox` and `processed_events` grow by one row per event, forever. The rules:
  - published outbox rows older than 7 days: Kafka has them (and consumers' processed_events guard
    against the rare re-send), the row has done its job
  - processed_events older than 30 days: they only matter while a redelivery is still possible. Kafka
    redelivers what it still holds, so keep these LONGER than the topics' retention (7 days)
  - service-specific jobs (refresh tokens, notification log) live in each service's app/maintenance.py

Deletes run in BATCHES (1000 rows, one short transaction each): one huge DELETE would hold locks and
bloat the write-ahead log for minutes while the service keeps serving traffic.

Jobs run as a background task in each service (like the relay and consumers): first run a minute after
startup, then every `every_seconds`. Two replicas running the same job at once is harmless (a row can
only be deleted once). Run all jobs now by hand: `docker compose exec <svc> python -m app.maintenance`."""

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from prometheus_client import Counter
from sqlalchemy import text

from smarthire_common.db import session_scope

log = logging.getLogger(__name__)

DAY = 24 * 3600
BATCH = 1000

RETENTION_DELETED = Counter("smarthire_retention_deleted_total", "Rows deleted by retention jobs", ["job"])


@dataclass(frozen=True)
class RetentionJob:
    name: str
    run: Callable[[], Awaitable[int]]  # deletes what's due, returns how many rows
    every_seconds: float = DAY


async def delete_in_batches(sql: str, **params) -> int:
    """Run a DELETE whose WHERE picks at most :batch rows (a LIMITed subquery) until nothing is left.
    One transaction per batch: locks are short and other writers barely notice."""
    total = 0
    while True:
        async with session_scope() as session:
            deleted = (await session.execute(text(sql), {**params, "batch": BATCH})).rowcount
        total += deleted
        if deleted < BATCH:
            return total
        await asyncio.sleep(0)  # let requests in between batches


def outbox_job(days: int = 7) -> RetentionJob:
    async def run() -> int:
        return await delete_in_batches(
            "DELETE FROM outbox WHERE id IN (SELECT id FROM outbox"
            " WHERE published_at < now() - make_interval(days => :days) ORDER BY id LIMIT :batch)", days=days)
    return RetentionJob("outbox", run)


def processed_events_job(days: int = 30) -> RetentionJob:
    async def run() -> int:
        # No single-column key: ctid is the row's physical address, good for "delete these rows" in one statement
        return await delete_in_batches(
            "DELETE FROM processed_events WHERE ctid IN (SELECT ctid FROM processed_events"
            " WHERE processed_at < now() - make_interval(days => :days) LIMIT :batch)", days=days)
    return RetentionJob("processed_events", run)


def mongo_outbox_job(days: int = 7) -> RetentionJob:
    """Mongo services (Scoring): published outbox documents older than `days`."""
    async def run() -> int:
        from datetime import UTC, datetime, timedelta

        from smarthire_common.mongo import get_db

        cutoff = datetime.now(UTC) - timedelta(days=days)
        result = await get_db()["outbox"].delete_many({"published_at": {"$ne": None, "$lt": cutoff}})
        return result.deleted_count
    return RetentionJob("outbox", run)


async def run_job(job: RetentionJob) -> int:
    deleted = await job.run()
    RETENTION_DELETED.labels(job.name).inc(deleted)
    log.info("retention %s: deleted %d rows", job.name, deleted)
    return deleted


async def run_all(jobs: list[RetentionJob]) -> dict[str, int]:
    return {job.name: await run_job(job) for job in jobs}


def start_retention(jobs: list[RetentionJob], *, first_delay: float = 60.0) -> list[asyncio.Task]:
    """Call in the service's lifespan; cancel the returned tasks on shutdown."""

    async def loop(job: RetentionJob) -> None:
        # Jitter: replicas (and services) restarted together don't all hit Postgres in the same second
        await asyncio.sleep(first_delay + random.uniform(0, 30))
        while True:
            try:
                await run_job(job)
            except Exception:
                log.exception("retention %s failed; next try in an hour", job.name)
                await asyncio.sleep(3600)
                continue
            await asyncio.sleep(job.every_seconds)

    return [asyncio.create_task(loop(job), name=f"retention-{job.name}") for job in jobs]
