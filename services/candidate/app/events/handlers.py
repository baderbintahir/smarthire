"""Reactions to other services' events (Step 7.6). Each handler runs inside the consumer's
transaction, right after the event id was recorded in processed_events (see smarthire_common.kafka),
so it runs once per event even if Kafka delivers it twice."""

import uuid

from smarthire_common.events import EventEnvelope
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import candidates as repo

CONSUMER_NAME = "candidate"  # the consumer group: replicas of this service share the work
TOPICS = ["identity.users"]


async def on_user_registered(session: AsyncSession, event: EventEnvelope) -> None:
    """A new CANDIDATE gets an empty profile (same id as their user). Other roles: nothing to do."""
    if event.payload["role"] != "CANDIDATE":
        return
    await repo.create_empty(session, uuid.UUID(event.payload["user_id"]))


HANDLERS = {"UserRegistered": on_user_registered}
