"""Reactions to events (Steps 7.6, 10.2). Each handler runs inside the consumer's transaction, right
after the event id was recorded in processed_events (see smarthire_common.kafka), so it runs once per
event even if Kafka delivers it twice."""

import asyncio
import uuid

from smarthire_common.celery import make_celery
from smarthire_common.events import EventEnvelope
from smarthire_common.kafka import RetryLater
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import candidates as repo
from app.tasks.names import PARSE_RESUME, QUEUE

CONSUMER_NAME = "candidate"  # the consumer group: replicas of this service share the work
TOPICS = ["identity.users", "candidates.profile"]  # the second is our own: ResumeUploaded -> parse it


async def on_user_registered(session: AsyncSession, event: EventEnvelope) -> None:
    """A new CANDIDATE gets an empty profile (same id as their user). Other roles: nothing to do."""
    if event.payload["role"] != "CANDIDATE":
        return
    await repo.create_empty(session, uuid.UUID(event.payload["user_id"]))


def build_handlers(settings) -> dict:
    producer = None  # a Celery app used only to SEND tasks (the worker container runs them)

    async def on_resume_uploaded(session: AsyncSession, event: EventEnvelope) -> None:
        """Queue the parsing task. Going through our own event (not enqueuing in the upload request)
        means a committed upload is never left unparsed: if RabbitMQ is down, we retry until it's back.
        A duplicate enqueue is harmless: the task skips resumes that are no longer UPLOADED."""
        nonlocal producer
        if producer is None:
            producer = make_celery("candidate-api", settings.celery_broker_url, default_queue=QUEUE)
        try:
            # send_task by NAME: the API doesn't import the worker's code. Blocking call -> thread
            await asyncio.to_thread(producer.send_task, PARSE_RESUME, args=[event.payload["resume_id"]], queue=QUEUE)
        except Exception as exc:
            raise RetryLater(f"task queue unavailable: {exc}") from exc

    return {"UserRegistered": on_user_registered, "ResumeUploaded": on_resume_uploaded}
