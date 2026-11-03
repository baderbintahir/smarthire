"""Use cases for jobs (Steps 5.3-5.4): orchestrate domain rules (app/domain/job.py) and
repositories. Each use case commits itself before returning (see CLAUDE.md: get_session's
commit runs after the response is sent)."""

import uuid

from smarthire_common.auth import CurrentUser
from smarthire_common.errors import ForbiddenError
from smarthire_common.events import new_event
from smarthire_common.outbox import add_outbox_event
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import job as rules
from app.domain.errors import JobNotFoundError, JobTransitionError, JobVersionConflictError
from app.domain.job import JobStatus
from app.models import Job, JobStage
from app.repositories import jobs as jobs_repo
from app.schemas.job import JobIn, JobUpdate


def _content(data: JobIn) -> dict:
    """The editable fields, normalized by the domain rules."""
    return {
        "title": data.title,
        "description": data.description,
        "location": data.location,
        "employment_type": data.employment_type,
        "required_skills": rules.normalize_skills(data.required_skills),
        "eligibility_rules": data.eligibility_rules.model_dump(exclude_none=True),
        "application_limit": data.application_limit,
        "deadline": data.deadline,
    }


async def _with_stages(session: AsyncSession, job: Job) -> tuple[Job, list[JobStage]]:
    return job, (await jobs_repo.stages_for(session, [job.id]))[job.id]


async def publish_event(session: AsyncSession, event_type: str, job: Job, payload: dict) -> None:
    """Queue an event about this job in the current transaction. Key = job id: all of one job's
    events land in the same partition, so consumers see them in order."""
    event = new_event(event_type, producer="job", payload=payload)
    await add_outbox_event(session, event, key=str(job.id))


def snapshot(job: Job, stages: list[JobStage]) -> dict:
    """The full job as other services need it (Application's job_replica, Phase 8). A full
    snapshot, not just "what changed": a consumer can upsert it without knowing earlier events."""
    return {
        "job_id": str(job.id),
        "recruiter_id": str(job.recruiter_id),
        "title": job.title,
        "status": job.status,
        "stages": [stage.name for stage in stages],  # in order: position = index
        "eligibility_rules": job.eligibility_rules,
        "application_limit": job.application_limit,
        "deadline": job.deadline.isoformat() if job.deadline else None,
        "version": job.version,
    }


async def request_publishing(session: AsyncSession, job: Job) -> None:
    """Ask for JobPublishingWorkflow to run for this job version, via the outbox, in the SAME
    transaction as the change to PROCESSING. Job's own consumer then starts the workflow with id
    publish-{job_id}-{version} (app/events/handlers.py). Starting it straight from this request
    would be a dual write: PROCESSING committed but no workflow (or the reverse) after a crash."""
    await publish_event(session, "JobPublishRequested", job, {"job_id": str(job.id), "version": job.version})


async def _load_owned(session: AsyncSession, job_id: uuid.UUID, user: CurrentUser) -> Job:
    """The job, if this recruiter owns it. Someone else's job: 403 if it's public anyway (READY or
    CLOSED, the id is no secret), but 404 for drafts, so ids of unpublished jobs can't be probed."""
    job = await jobs_repo.get(session, job_id)
    if job is None:
        raise JobNotFoundError()
    if job.recruiter_id != user.user_id:
        if job.status in (JobStatus.READY, JobStatus.CLOSED):
            raise ForbiddenError("You can only change your own jobs")
        raise JobNotFoundError()
    return job


async def create_job(session: AsyncSession, user: CurrentUser, data: JobIn) -> tuple[Job, list[JobStage]]:
    stages = rules.normalize_stages(data.stages)
    # recruiter_id comes from the verified token, never from the request body
    job = Job(recruiter_id=user.user_id, **_content(data))
    await jobs_repo.add(session, job, stages)
    await session.commit()
    await session.refresh(job)  # load server defaults (status, version, timestamps)
    return await _with_stages(session, job)


async def get_job(session: AsyncSession, job_id: uuid.UUID, user: CurrentUser | None) -> tuple[Job, list[JobStage]]:
    """Published jobs (READY/CLOSED) are visible to anyone; drafts only to their owner. For
    everyone else an unpublished job doesn't exist (404, not 403)."""
    job = await jobs_repo.get(session, job_id)
    visible = job is not None and (
        job.status in (JobStatus.READY, JobStatus.CLOSED) or (user is not None and job.recruiter_id == user.user_id)
    )
    if not visible:
        raise JobNotFoundError()
    return await _with_stages(session, job)


async def _page_with_stages(session: AsyncSession, jobs: list[Job]) -> list[tuple[Job, list[JobStage]]]:
    stages = await jobs_repo.stages_for(session, [job.id for job in jobs])
    return [(job, stages[job.id]) for job in jobs]


async def list_public(session: AsyncSession, *, limit: int, cursor: str | None, employment_type: str | None):
    jobs, next_cursor = await jobs_repo.list_ready(session, limit=limit, cursor=cursor, employment_type=employment_type)
    return await _page_with_stages(session, jobs), next_cursor


async def list_mine(session: AsyncSession, user: CurrentUser, *, status: str | None, limit: int, cursor: str | None):
    jobs, next_cursor = await jobs_repo.list_for_recruiter(
        session, user.user_id, status=status, limit=limit, cursor=cursor
    )
    return await _page_with_stages(session, jobs), next_cursor


async def update_job(
    session: AsyncSession, user: CurrentUser, job_id: uuid.UUID, data: JobUpdate
) -> tuple[Job, list[JobStage]]:
    job = await _load_owned(session, job_id, user)
    new_status = rules.status_after_edit(job.status)  # READY -> PROCESSING: content must be re-processed
    stages = rules.normalize_stages(data.stages)
    # The check and the write are ONE statement: between our read above and this UPDATE, another
    # recruiter could have saved; the version condition catches that
    if not await jobs_repo.update_if_version(session, job_id, data.version, status=new_status, **_content(data)):
        raise JobVersionConflictError()
    await jobs_repo.replace_stages(session, job_id, stages)
    await session.refresh(job)  # the updated row (new version, status), still inside our transaction
    job, saved_stages = await _with_stages(session, job)
    # Same transaction as the update: the event exists if and only if the change does
    await publish_event(session, "JobUpdated", job, snapshot(job, saved_stages))
    if job.status == JobStatus.PROCESSING:  # a READY job was edited: its content must be processed again
        await request_publishing(session, job)
    await session.commit()
    return job, saved_stages


async def publish_job(session: AsyncSession, user: CurrentUser, job_id: uuid.UUID) -> Job:
    """DRAFT/FAILED -> PROCESSING, and request the publishing workflow (Step 9.5), which takes the
    job to READY (or FAILED). The caller gets 202 right away: the work happens afterwards."""
    job = await _load_owned(session, job_id, user)
    target = rules.publish(job.status)
    if not await jobs_repo.set_status_if(session, job_id, expected=job.status, new=target):
        raise JobTransitionError(job.status, target)  # someone changed the status meanwhile
    await session.refresh(job)  # the new version (every status change bumps it)
    await request_publishing(session, job)
    await session.commit()
    return job


async def close_job(session: AsyncSession, user: CurrentUser, job_id: uuid.UUID) -> tuple[Job, list[JobStage]]:
    job = await _load_owned(session, job_id, user)
    target = rules.close(job.status)
    if not await jobs_repo.set_status_if(session, job_id, expected=job.status, new=target):
        raise JobTransitionError(job.status, target)
    await session.refresh(job)
    # Application closes the job's open applications when it sees this (Phase 8)
    await publish_event(session, "JobClosed", job,
                   {"job_id": str(job.id), "recruiter_id": str(job.recruiter_id), "version": job.version})
    await session.commit()
    return await _with_stages(session, job)
