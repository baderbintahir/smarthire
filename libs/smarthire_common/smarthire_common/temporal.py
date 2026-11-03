"""Temporal client and worker helpers (Phase 9). Needs the `temporal` extra: smarthire-common[temporal].

A service must start even while Temporal is down (like Kafka: the API keeps working). So the
worker connects in the background and keeps retrying, instead of failing the app's startup."""

import asyncio
import logging
from collections.abc import Sequence

from temporalio.client import Client
from temporalio.worker import Worker

log = logging.getLogger(__name__)

_client: Client | None = None
_lock = asyncio.Lock()


async def connect(settings, *, retry_seconds: float = 5.0) -> Client:
    """The process-wide Temporal client, connecting (and retrying until it works) on first use."""
    global _client
    async with _lock:  # concurrent first callers share one connection attempt
        while _client is None:
            try:
                _client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
                log.info("connected to Temporal at %s", settings.temporal_address)
            except Exception as exc:
                log.warning("Temporal not reachable (%s); retrying in %ss", exc, retry_seconds)
                await asyncio.sleep(retry_seconds)
    return _client


def start_worker(settings, *, task_queue: str, workflows: Sequence = (), activities: Sequence = ()) -> asyncio.Task:
    """Run a worker for this task queue as a background task (cancel it on shutdown). A worker POLLS
    Temporal for tasks of its queue; nothing calls into this service, so it needs no open port."""

    async def run() -> None:
        while True:
            client = await connect(settings)
            try:
                log.info("Temporal worker started on %s", task_queue)
                await Worker(client, task_queue=task_queue, workflows=list(workflows), activities=list(activities)).run()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Temporal worker on %s stopped; restarting in 5 s", task_queue)
                await asyncio.sleep(5)

    return asyncio.create_task(run(), name=f"temporal-worker-{task_queue}")


def reset_client() -> None:
    """Tests: forget the cached client."""
    global _client
    _client = None
