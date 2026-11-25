"""Data retention: scheduled clean-up of rows that are no longer needed (Step 14.3b, Appendix E.14).

Each service lists its own jobs in app/maintenance.py (e.g. Notification: sent emails older than 90 days).
Design v2.0 removed the outbox and processed_events tables, and with them their clean-up jobs.

Deletes run in BATCHES (1000 rows, one short transaction each): one huge DELETE would hold locks and
bloat the write-ahead log for minutes while the service keeps serving traffic.

Jobs run as a background task in each service: first run a minute after
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
