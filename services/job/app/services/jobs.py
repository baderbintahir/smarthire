"""Use cases for jobs (Steps 5.3-5.4): orchestrate domain rules (app/domain/job.py) and
repositories. Each use case commits itself before returning (see CLAUDE.md: get_session's
commit runs after the response is sent)."""

import uuid

from smarthire_common.auth import CurrentUser
from smarthire_common.errors import ForbiddenError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import job as rules
from app.domain.errors import JobNotFoundError, JobVersionConflictError
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
    session: AsyncSession, orchestrator, user: CurrentUser, job_id: uuid.UUID, data: JobUpdate
) -> tuple[Job, list[JobStage]]:
    job = await _load_owned(session, job_id, user)
    new_status = rules.status_after_edit(job.status)  # READY -> PROCESSING: content must be re-processed
    stages = rules.normalize_stages(data.stages)
    if new_status == JobStatus.PROCESSING:
        # A published job: others (Application's replica, the structured job) must follow the edit, so it
        # runs as JobPublishingWorkflow (design v2.1). Its commit step saves the edit (publishing.start_processing)
        await session.rollback()  # hold no transaction while the orchestrator calls us back
        await orchestrator.run_action(
            "edit-ready-job", key={"job_id": str(job_id), "version": data.version},
            input={"job_id": str(job_id), "expected_version": data.version,
                   "content": jsonable_content(data), "stages": stages})
        return await _with_stages(session, await jobs_repo.get(session, job_id))
    # A draft (or a failed publish): nobody else knows it yet, so a plain write
    # The check and the write are ONE statement: between our read above and this UPDATE, another
    # recruiter could have saved; the version condition catches that
    if not await jobs_repo.is_job_updated_at_version(session, job_id, data.version, status=new_status, **_content(data)):
        raise JobVersionConflictError()
    await jobs_repo.replace_stages(session, job_id, stages)
    await session.commit()
    await session.refresh(job)
    return await _with_stages(session, job)


async def publish_job(session: AsyncSession, orchestrator, user: CurrentUser, job_id: uuid.UUID) -> dict:
    """DRAFT/FAILED -> PROCESSING as the first step of JobPublishingWorkflow (design v2.1), which then
    takes the job to READY (or FAILED). The caller gets 202 once PROCESSING is committed."""
    job = await _load_owned(session, job_id, user)
    rules.publish(job.status)  # friendly 409 without starting anything; checked again in the commit step
    version = job.version  # read before the rollback: it expires ORM objects (MissingGreenlet otherwise)
    await session.rollback()
    return await orchestrator.run_action("publish-job", key={"job_id": str(job_id), "version": version},
                                         input={"job_id": str(job_id), "expected_version": version})


async def close_job(session: AsyncSession, orchestrator, user: CurrentUser,
                    job_id: uuid.UUID) -> tuple[Job, list[JobStage]]:
    """READY -> CLOSED as the first step of JobClosingWorkflow, which then closes the job's open
    applications in Application and stops their workflows."""
    job = await _load_owned(session, job_id, user)
    rules.close(job.status)
    version = job.version  # read before the rollback (it expires ORM objects)
    await session.rollback()
    await orchestrator.run_action("close-job", key={"job_id": str(job_id)},
                                  input={"job_id": str(job_id), "expected_version": version})
    return await _with_stages(session, await jobs_repo.get(session, job_id))


def jsonable_content(data: JobIn) -> dict:
    """_content as JSON (it travels through the orchestrator to our own commit step)."""
    content = _content(data)
    content["deadline"] = content["deadline"].isoformat() if content["deadline"] else None
    return content
