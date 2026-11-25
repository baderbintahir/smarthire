"""Job's workflow steps (design v2.1): called by the orchestrator's JobPublishingWorkflow and
JobClosingWorkflow through /internal/jobs/*. Each is one transaction and safe to run twice (Temporal
repeats a step after a timeout): "already done" answers like the first time.

Every status change bumps the version, so a version tells exactly which change a step is about:
    start_processing(v)   DRAFT/FAILED (publish) or READY (edit) at v  ->  PROCESSING at v+1
    finish(v, READY|FAILED)  PROCESSING at v  ->  READY / FAILED at v+1
    close(v)              READY at v  ->  CLOSED at v+1"""

import uuid
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import job as rules
from app.domain.errors import JobNotFoundError, JobTransitionError, JobVersionConflictError, StaleJobVersionError
from app.domain.job import JobStatus
from app.models import Job
from app.repositories import jobs as jobs_repo
from app.services.jobs import snapshot


async def _job(session: AsyncSession, job_id: uuid.UUID) -> Job:
    job = await jobs_repo.get(session, job_id)
    if job is None:
        raise JobNotFoundError()
    return job


async def view(session: AsyncSession, job: Job) -> dict:
    """The snapshot other services need (Application's replica), plus what enrichment needs."""
    stages = (await jobs_repo.stages_for(session, [job.id]))[job.id]
    return {**snapshot(job, stages), "description": job.description, "required_skills": job.required_skills}


async def start_processing(session: AsyncSession, job_id: uuid.UUID, expected_version: int, *,
                           content: dict | None = None, stages: list[str] | None = None) -> dict:
    """The commit step of JobPublishingWorkflow. content=None: publish a DRAFT/FAILED job; content given:
    the edit of a READY job (saved here, in the same transaction as PROCESSING)."""
    job = await _job(session, job_id)
    if job.status == JobStatus.PROCESSING and job.version == expected_version + 1:
        return await view(session, job)  # a retried step: already done
    if job.version != expected_version:
        raise JobVersionConflictError()
    if content is not None:
        if rules.status_after_edit(job.status) != JobStatus.PROCESSING:
            raise JobTransitionError(job.status, JobStatus.PROCESSING)  # only READY edits are workflow steps
        values = {**content, "deadline": datetime.fromisoformat(content["deadline"]) if content["deadline"] else None}
        changed = await jobs_repo.update_if_version(session, job_id, expected_version, status=JobStatus.PROCESSING,
                                                    **values)
        if changed:
            await jobs_repo.replace_stages(session, job_id, stages)
    else:
        rules.publish(job.status)  # DRAFT or FAILED only (409 otherwise)
        changed = await jobs_repo.update_if_version(session, job_id, expected_version, status=JobStatus.PROCESSING)
    if not changed:
        raise JobVersionConflictError()  # someone saved between our read and the update
    await session.commit()
    await session.refresh(job)
    return await view(session, job)


async def finish(session: AsyncSession, job_id: uuid.UUID, version: int, status: str) -> dict:
    """mark_ready / mark_failed: PROCESSING -> READY or FAILED, only for the version this workflow made."""
    if await jobs_repo.finish_publishing(session, job_id, version, status):
        await session.commit()
    job = await _job(session, job_id)
    await session.refresh(job)
    if job.status != status or job.version != version + 1:
        raise StaleJobVersionError(job_id, version)  # edited or re-published since: a newer run owns it
    return await view(session, job)


async def close(session: AsyncSession, job_id: uuid.UUID, expected_version: int) -> dict:
    """The commit step of JobClosingWorkflow: READY -> CLOSED (final)."""
    job = await _job(session, job_id)
    if job.status == JobStatus.CLOSED and job.version == expected_version + 1:
        return await view(session, job)  # a retried step
    if job.version != expected_version:
        raise JobVersionConflictError()
    rules.close(job.status)
    if not await jobs_repo.update_if_version(session, job_id, expected_version, status=JobStatus.CLOSED):
        raise JobVersionConflictError()
    await session.commit()
    await session.refresh(job)
    return await view(session, job)
