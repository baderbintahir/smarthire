"""Celery helpers (Phase 10). Needs the `celery` extra: smarthire-common[celery].

Celery = fire-and-forget background tasks through a queue (RabbitMQ): "parse this resume", "send this
email". One step, retried on failure, no orchestration (that's Temporal's job, rule 5)."""

import asyncio

from celery import Celery


def make_celery(name: str, broker_url: str, *, default_queue: str) -> Celery:
    app = Celery(name, broker=broker_url)
    app.conf.update(
        task_default_queue=default_queue,  # each service has its own queue (and its own workers)
        # acks_late: the broker deletes a task only AFTER it finished. If a worker dies mid-task, the
        # task goes back to the queue and another worker runs it (so tasks must be idempotent)
        task_acks_late=True,
        task_reject_on_worker_lost=True,  # ...including when the worker process is killed
        worker_prefetch_multiplier=1,  # take one task at a time: a slow task doesn't hoard others
        task_serializer="json",
        accept_content=["json"],
        task_ignore_result=True,  # fire-and-forget: nobody waits for return values (no result backend)
        broker_connection_retry_on_startup=True,
    )
    return app


_loop: asyncio.AbstractEventLoop | None = None


def run_async(coro):
    """Run async code (our DB/storage helpers) from a synchronous Celery task. ONE event loop per
    worker process, reused by every task: the DB engine's pooled connections belong to the loop that
    created them, so a new loop per task (asyncio.run) would break them. Created lazily, in the
    worker child process, never in the parent before it forks."""
    global _loop
    if _loop is None:
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
    return _loop.run_until_complete(coro)
