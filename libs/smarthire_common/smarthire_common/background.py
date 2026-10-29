"""Helpers for background tasks that live as long as the app (outbox relay, Kafka consumers)."""

import asyncio
from contextlib import suppress


async def stop_tasks(*tasks: asyncio.Task) -> None:
    """Cancel the tasks and wait until they have finished cleaning up (e.g. leaving the consumer group)."""
    for task in tasks:
        task.cancel()
    for task in tasks:
        with suppress(asyncio.CancelledError):
            await task
