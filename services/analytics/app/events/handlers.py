"""Kafka events -> facts (Step 12.2, kept until Kafka is removed in Step T10). Each event is recorded as
a fact with the event id as its fact id, so the projections have ONE writer path (record_fact) whether a
fact comes from a workflow or from an event that hasn't moved to a workflow yet."""

from smarthire_common.events import EventEnvelope
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.facts import APPLIERS, is_interview_stage, record_fact  # noqa: F401  (is_interview_stage: tests)

CONSUMER_NAME = "analytics"
TOPICS = ["identity.users", "jobs.lifecycle", "applications.lifecycle", "scoring.results", "system.failures",
          "assistant.usage"]


async def on_event(session: AsyncSession, event: EventEnvelope) -> None:
    await record_fact(session, fact_id=str(event.event_id), kind=event.event_type, occurred_at=event.occurred_at,
                      data=event.payload)


HANDLERS = {kind: on_event for kind in APPLIERS}
