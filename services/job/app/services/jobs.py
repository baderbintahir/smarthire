"""Use cases for jobs (Steps 5.3-5.4): orchestrate domain rules (app/domain/job.py) and
repositories. Each use case commits itself before returning (see CLAUDE.md: get_session's
commit runs after the response is sent)."""

import uuid

from smarthire_common.auth import CurrentUser
from smarthire_common.errors import ForbiddenError
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
    # TODO(Phase 7.7): JobUpdated outbox row; TODO(Phase 9.5): re-run publishing when READY -> PROCESSING
    await session.commit()
    await session.refresh(job)
    return await _with_stages(session, job)


async def publish_job(session: AsyncSession, user: CurrentUser, job_id: uuid.UUID) -> Job:
    """Step 5.4 stub: DRAFT/FAILED -> PROCESSING and stop there. Phase 9 adds the workflow that
    takes it to READY (or FAILED)."""
    job = await _load_owned(session, job_id, user)
    target = rules.publish(job.status)
    if not await jobs_repo.set_status_if(session, job_id, expected=job.status, new=target):
        raise JobTransitionError(job.status, target)  # someone changed the status meanwhile
    # TODO(Phase 9.5): JobPublishRequested outbox row, which starts the publishing workflow
    await session.commit()
    await session.refresh(job)
    return job


async def close_job(session: AsyncSession, user: CurrentUser, job_id: uuid.UUID) -> tuple[Job, list[JobStage]]:
    job = await _load_owned(session, job_id, user)
    target = rules.close(job.status)
    if not await jobs_repo.set_status_if(session, job_id, expected=job.status, new=target):
        raise JobTransitionError(job.status, target)
    # TODO(Phase 7.7): JobClosed outbox row (Application closes open applications)
    await session.commit()
    await session.refresh(job)
    return await _with_stages(session, job)
