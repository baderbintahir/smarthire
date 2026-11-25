"""Application's copy of each job (Step 8.2; design v2.1: kept current by the orchestrator's job workflows
through /internal/job-replicas/*, instead of Job's Kafka events).

Both steps are safe to run twice: the source_version guard ignores a snapshot that isn't newer than the
one stored, so a retried or late step can't roll the replica back."""

import logging
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import applications as applications_repo
from app.repositories import job_replica
from app.workflows.activities import SYSTEM_ACTOR

log = logging.getLogger(__name__)


async def sync(session: AsyncSession, snapshot: dict) -> bool:
    """sync_job_replica step: insert or update from Job's snapshot, if newer. False = ignored (stale)."""
    updated = await job_replica.upsert_if_newer(session, snapshot)
    await session.commit()
    if not updated:
        log.info("ignored stale snapshot v%s for job %s", snapshot["version"], snapshot["job_id"])
    return updated


async def close_job(session: AsyncSession, job_id: uuid.UUID, version: int) -> list[dict]:
    """close_job_applications step (Step 10.5): the replica CLOSED, the job's SUBMITTED applications
    CLOSED (freeing the candidates' quota). Returns the closed applications, whose workflows the
    orchestrator then tells to stop. A repeated step finds the replica closed already: nothing to do."""
    if not await job_replica.close_if_newer(session, job_id, version):
        return []
    closed = await applications_repo.close_submitted(session, job_id)
    for application_id, candidate_id, stage in closed:
        await applications_repo.add_history(session, application_id, from_stage=stage, to_stage="CLOSED",
                                            changed_by=SYSTEM_ACTOR, reason="job closed")
        await applications_repo.change_quota(session, candidate_id, -1)
    await session.commit()
    return [{"application_id": str(a), "candidate_id": str(c)} for a, c, _ in closed]
