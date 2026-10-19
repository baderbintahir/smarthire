"""HTTP layer for /jobs. Only HTTP concerns here: parse input, check who's calling, call a use
case, shape the response. Rules live in app/domain, orchestration in app/services."""

import uuid
from typing import Literal

from fastapi import APIRouter, Depends, Query, status
from smarthire_common.auth import CurrentUser, get_optional_user, require_role
from smarthire_common.db import get_session
from smarthire_common.errors import ForbiddenError, UnauthorizedError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Job, JobStage
from app.schemas.job import JobIn, JobOut, JobPage, JobUpdate, PublishAccepted, StageOut
from app.services import jobs as service

router = APIRouter(prefix="/jobs", tags=["jobs"])

recruiter = require_role("RECRUITER")


def to_out(job: Job, stages: list[JobStage]) -> JobOut:
    return JobOut(
        **{field: getattr(job, field) for field in JobOut.model_fields if field != "stages"},
        stages=[StageOut(name=s.name, position=s.position) for s in stages],
    )


@router.post("", status_code=status.HTTP_201_CREATED, response_model=JobOut)
async def create_job(body: JobIn, user: CurrentUser = Depends(recruiter), session: AsyncSession = Depends(get_session)):
    return to_out(*await service.create_job(session, user, body))


@router.get("", response_model=JobPage)
async def list_jobs(
    mine: bool = False,  # false: public board (READY only); true: my jobs, any status (recruiters)
    status_filter: Literal["DRAFT", "PROCESSING", "READY", "FAILED", "CLOSED"] | None = Query(None, alias="status"),
    employment_type: Literal["FULL_TIME", "PART_TIME", "CONTRACT", "INTERNSHIP"] | None = None,
    limit: int = Query(20, ge=1, le=100),
    cursor: str | None = None,
    user: CurrentUser | None = Depends(get_optional_user),
    session: AsyncSession = Depends(get_session),
):
    if mine:
        if user is None:
            raise UnauthorizedError("Log in to see your jobs")
        if user.role != "RECRUITER":
            raise ForbiddenError("Only recruiters have jobs")
        items, next_cursor = await service.list_mine(session, user, status=status_filter, limit=limit, cursor=cursor)
    else:
        items, next_cursor = await service.list_public(
            session, limit=limit, cursor=cursor, employment_type=employment_type
        )
    return JobPage(items=[to_out(job, stages) for job, stages in items], next_cursor=next_cursor)


@router.get("/{job_id}", response_model=JobOut)
async def get_job(
    job_id: uuid.UUID,
    user: CurrentUser | None = Depends(get_optional_user),
    session: AsyncSession = Depends(get_session),
):
    return to_out(*await service.get_job(session, job_id, user))


@router.put("/{job_id}", response_model=JobOut)
async def update_job(
    job_id: uuid.UUID, body: JobUpdate, user: CurrentUser = Depends(recruiter), session: AsyncSession = Depends(get_session)
):
    return to_out(*await service.update_job(session, user, job_id, body))


@router.post("/{job_id}/publish", status_code=status.HTTP_202_ACCEPTED, response_model=PublishAccepted)
async def publish_job(job_id: uuid.UUID, user: CurrentUser = Depends(recruiter), session: AsyncSession = Depends(get_session)):
    # 202 Accepted: the work (enrichment, Phase 9) happens after we answer
    job = await service.publish_job(session, user, job_id)
    return PublishAccepted(id=job.id, status=job.status)


@router.post("/{job_id}/close", response_model=JobOut)
async def close_job(job_id: uuid.UUID, user: CurrentUser = Depends(recruiter), session: AsyncSession = Depends(get_session)):
    return to_out(*await service.close_job(session, user, job_id))
