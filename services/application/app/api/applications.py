"""HTTP layer for applications. Only HTTP concerns here: parse input, check who's calling, call a
use case, shape the response. Rules live in app/domain, orchestration in app/services."""

import uuid

from fastapi import APIRouter, Depends, Header, Query, Request, status
from fastapi.responses import JSONResponse
from smarthire_common.auth import CurrentUser, require_role
from smarthire_common.db import get_session
from smarthire_common.redis import get_redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.application import (
    ApplicationDetail,
    ApplicationOut,
    ApplicationPage,
    ApplyIn,
    BoardPage,
    DecisionIn,
    NoteIn,
    NoteOut,
    StageChangeIn,
    StageEntryOut,
    application_out,
)
from app.services import applications as service
from app.services.idempotency import IdempotencyStore

router = APIRouter(tags=["applications"])

candidate_only = require_role("CANDIDATE")  # step 2 of the apply flow: role must be CANDIDATE
recruiter_only = require_role("RECRUITER")
anyone = require_role("CANDIDATE", "RECRUITER")


@router.post("/applications", status_code=status.HTTP_202_ACCEPTED, response_model=ApplicationOut)
async def apply(
    body: ApplyIn,
    request: Request,
    # Required: the client generates a unique key per apply ATTEMPT and reuses it on retries
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
    user: CurrentUser = Depends(candidate_only),
    session: AsyncSession = Depends(get_session),
):
    # 202 Accepted: the application exists, but scoring and screening happen afterwards (Phase 10)
    settings = request.app.state.settings
    result = await service.apply(
        session, user, body.job_id,
        idempotency_key=idempotency_key,
        idempotency=IdempotencyStore(get_redis(), ttl_seconds=settings.idempotency_ttl_seconds),
        authorization=request.headers["authorization"],  # forwarded to Candidate: it checks the same user
        candidates=request.app.state.candidates,
        max_active=settings.max_active_applications,
    )
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)


@router.get("/applications/me", response_model=ApplicationPage)
async def my_applications(
    limit: int = Query(20, ge=1, le=100),
    cursor: str | None = None,
    user: CurrentUser = Depends(candidate_only),
    session: AsyncSession = Depends(get_session),
):
    rows, next_cursor = await service.mine(session, user, limit=limit, cursor=cursor)
    return ApplicationPage(items=[application_out(a, j) for a, j in rows], next_cursor=next_cursor)


@router.get("/jobs/{job_id}/applications", response_model=BoardPage)
async def job_board(
    job_id: uuid.UUID,
    stage: str | None = None,  # filter by current stage
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),  # a job's applicants are a bounded list: offset is fine here
    user: CurrentUser = Depends(recruiter_only),
    session: AsyncSession = Depends(get_session),
):
    applications, job = await service.board(session, user, job_id, stage=stage, limit=limit, offset=offset)
    return BoardPage(items=[application_out(a, job) for a in applications])


@router.get("/applications/{application_id}", response_model=ApplicationDetail)
async def application_detail(
    application_id: uuid.UUID, user: CurrentUser = Depends(anyone), session: AsyncSession = Depends(get_session)
):
    application, job, history, notes = await service.detail(session, user, application_id)
    return ApplicationDetail(
        **application_out(application, job).model_dump(),
        history=[StageEntryOut.model_validate(h) for h in history],
        notes=[NoteOut.model_validate(n) for n in notes] if notes is not None else None,
    )


@router.patch("/applications/{application_id}/stage", response_model=ApplicationOut)
async def change_stage(
    application_id: uuid.UUID, body: StageChangeIn, user: CurrentUser = Depends(recruiter_only),
    session: AsyncSession = Depends(get_session),
):
    return application_out(*await service.change_stage(
        session, user, application_id, to_stage=body.to_stage, version=body.version, reason=body.reason))


@router.post("/applications/{application_id}/decision", response_model=ApplicationOut)
async def decide(
    application_id: uuid.UUID, body: DecisionIn, user: CurrentUser = Depends(recruiter_only),
    session: AsyncSession = Depends(get_session),
):
    return application_out(*await service.decide(
        session, user, application_id, decision=body.decision, version=body.version, reason=body.reason))


@router.post("/applications/{application_id}/notes", status_code=status.HTTP_201_CREATED, response_model=NoteOut)
async def add_note(
    application_id: uuid.UUID, body: NoteIn, user: CurrentUser = Depends(recruiter_only),
    session: AsyncSession = Depends(get_session),
):
    return NoteOut.model_validate(await service.add_note(session, user, application_id, body.body))


@router.post("/applications/{application_id}/withdraw", response_model=ApplicationOut)
async def withdraw(
    application_id: uuid.UUID, request: Request, user: CurrentUser = Depends(candidate_only),
    session: AsyncSession = Depends(get_session),
):
    settings = request.app.state.settings
    return application_out(*await service.withdraw(
        session, user, application_id, settings=settings if settings.temporal_enabled else None))
