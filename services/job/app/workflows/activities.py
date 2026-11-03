"""Job's own activities for JobPublishingWorkflow, on task queue `job-tq` (they touch Job's DB, so
they run in Job's worker). Idempotent: Temporal may run each more than once, so "already done"
counts as success, and anything else that doesn't fit means the run is stale."""

import uuid

from smarthire_common.db import session_scope
from temporalio import activity
from temporalio.exceptions import ApplicationError

from app.domain.job import JobStatus
from app.repositories import jobs as jobs_repo
from app.services.jobs import publish_event, snapshot
from app.workflows.publishing import STALE


def _stale(job_id: str, version: int) -> ApplicationError:
    # non_retryable: retrying can't make an old version current again
    return ApplicationError(f"job {job_id} is no longer at version {version}", type=STALE, non_retryable=True)


@activity.defn(name="set_processing")
async def set_processing(job_id: str, version: int) -> dict:
    """The API already set PROCESSING (with the JobPublishRequested event, one transaction). This
    confirms the job is STILL at this version and processing, and returns the content to enrich."""
    async with session_scope() as session:
        job = await jobs_repo.get(session, uuid.UUID(job_id))
        if job is None or job.version != version or job.status != JobStatus.PROCESSING:
            raise _stale(job_id, version)
        return {"description": job.description, "required_skills": job.required_skills}


async def _finish(job_id: str, version: int, status: str, event_type: str, payload: dict | None = None) -> None:
    async with session_scope() as session:
        if not await jobs_repo.finish_publishing(session, uuid.UUID(job_id), version, status):
            job = await jobs_repo.get(session, uuid.UUID(job_id))
            if job is not None and job.status == status and job.version == version + 1:
                return  # this activity already ran (its result got lost): done, nothing to repeat
            raise _stale(job_id, version)
        job = await jobs_repo.get(session, uuid.UUID(job_id))
        await session.refresh(job)
        stages = (await jobs_repo.stages_for(session, [job.id]))[job.id]
        # Same transaction as the status change: the event exists if and only if the change does
        await publish_event(session, event_type, job, payload or snapshot(job, stages))


@activity.defn(name="mark_ready")
async def mark_ready(job_id: str, version: int) -> None:
    """PROCESSING -> READY and JobReady (full snapshot: Application's replica starts accepting applies)."""
    await _finish(job_id, version, JobStatus.READY, "JobReady")


@activity.defn(name="mark_failed")
async def mark_failed(job_id: str, version: int, reason: str) -> None:
    """PROCESSING -> FAILED and JobPublishFailed (Notification tells the recruiter, Phase 11)."""
    async with session_scope() as session:
        job = await jobs_repo.get(session, uuid.UUID(job_id))
        recruiter_id = str(job.recruiter_id) if job else None
    await _finish(job_id, version, JobStatus.FAILED, "JobPublishFailed",
                  {"job_id": job_id, "recruiter_id": recruiter_id, "version": version + 1, "reason": reason})


ACTIVITIES = [set_processing, mark_ready, mark_failed]
