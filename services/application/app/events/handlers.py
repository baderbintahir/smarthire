"""Keeping job_replica up to date from Job's events (Step 8.2). Each handler runs inside the
consumer's transaction (processed_events + handler, see smarthire_common.kafka)."""

import logging
import uuid

from smarthire_common.events import EventEnvelope
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import job_replica

log = logging.getLogger(__name__)

CONSUMER_NAME = "application"  # the consumer group
TOPICS = ["jobs.lifecycle"]


async def on_job_snapshot(session: AsyncSession, event: EventEnvelope) -> None:
    """JobUpdated and JobReady carry the whole job: insert or update, if newer than what we have."""
    if not await job_replica.upsert_if_newer(session, event.payload):
        log.info("ignored stale %s v%s for job %s", event.event_type, event.payload["version"], event.payload["job_id"])


async def on_job_closed(session: AsyncSession, event: EventEnvelope) -> None:
    # TODO(Phase 10.5): also close the job's open applications and signal their workflows
    job_id = uuid.UUID(event.payload["job_id"])
    if not await job_replica.close_if_newer(session, job_id, event.payload["version"]):
        log.info("ignored JobClosed v%s for job %s (stale or unknown job)", event.payload["version"], job_id)


HANDLERS = {"JobUpdated": on_job_snapshot, "JobReady": on_job_snapshot, "JobClosed": on_job_closed}
