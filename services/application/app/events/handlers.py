"""Application's event handlers (Steps 8.2, 10.4, 10.5). Each runs inside the consumer's transaction,
right after its event id was recorded in processed_events (see smarthire_common.kafka)."""

import logging
import uuid

from smarthire_common.events import EventEnvelope
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import applications as applications_repo
from app.repositories import job_replica
from app.services import workflows
from app.workflows.activities import SYSTEM_ACTOR

log = logging.getLogger(__name__)

CONSUMER_NAME = "application"  # the consumer group
TOPICS = ["jobs.lifecycle", "applications.lifecycle"]  # Job's events + our own (to start workflows)


async def on_job_snapshot(session: AsyncSession, event: EventEnvelope) -> None:
    """JobUpdated and JobReady carry the whole job: insert or update, if newer than what we have."""
    if not await job_replica.upsert_if_newer(session, event.payload):
        log.info("ignored stale %s v%s for job %s", event.event_type, event.payload["version"], event.payload["job_id"])


def build_handlers(settings) -> dict:
    async def on_job_closed(session: AsyncSession, event: EventEnvelope) -> None:
        """Step 10.5: update the replica, close the job's SUBMITTED applications (freeing the candidates'
        quota), and tell their running workflows to stop."""
        job_id = uuid.UUID(event.payload["job_id"])
        if not await job_replica.close_if_newer(session, job_id, event.payload["version"]):
            log.info("ignored JobClosed v%s for job %s (stale or unknown job)", event.payload["version"], job_id)
            return
        closed = await applications_repo.close_submitted(session, job_id)
        for application_id, candidate_id, stage in closed:
            await applications_repo.add_history(session, application_id, from_stage=stage, to_stage="CLOSED",
                                                changed_by=SYSTEM_ACTOR, reason="job closed")
            await applications_repo.change_quota(session, candidate_id, -1)
        await session.commit()  # signal only once the closing is saved (signals are best effort)
        for application_id, _, _ in closed:
            await workflows.signal(settings, str(application_id), "job_closed")

    async def on_application_submitted(session: AsyncSession, event: EventEnvelope) -> None:
        """Step 10.4: our own event starts the application's workflow (workflow id app-{id})."""
        await workflows.start(settings, event.payload["application_id"])

    return {
        "JobUpdated": on_job_snapshot,
        "JobReady": on_job_snapshot,
        "JobClosed": on_job_closed,
        "ApplicationSubmitted": on_application_submitted,
    }
