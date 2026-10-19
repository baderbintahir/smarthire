"""HTTP layer for /candidates. Only HTTP concerns here: parse input, check who's calling, call a
use case, shape the response. Rules live in app/domain, orchestration in app/services."""

import uuid

from fastapi import APIRouter, Depends, File, Request, UploadFile, status
from smarthire_common.auth import CurrentUser, get_current_user, require_role
from smarthire_common.db import get_session
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Candidate, Resume
from app.schemas.candidate import ProfileIn, ProfileOut, ResumeList, ResumeOut
from app.services import candidates as service

router = APIRouter(prefix="/candidates", tags=["candidates"])

candidate_only = require_role("CANDIDATE")


def to_out(candidate: Candidate, active: Resume | None) -> ProfileOut:
    return ProfileOut(
        id=candidate.id,
        full_name=candidate.full_name,
        headline=candidate.headline,
        skills=candidate.skills,
        experience_years=candidate.experience_years,
        education=candidate.education,
        active_resume=ResumeOut.model_validate(active) if active else None,
        created_at=candidate.created_at,
        updated_at=candidate.updated_at,
    )


@router.get("/me", response_model=ProfileOut)
async def my_profile(user: CurrentUser = Depends(candidate_only), session: AsyncSession = Depends(get_session)):
    return to_out(*await service.get_profile(session, user.user_id))


@router.put("/me", response_model=ProfileOut)
async def save_my_profile(
    body: ProfileIn, user: CurrentUser = Depends(candidate_only), session: AsyncSession = Depends(get_session)
):
    return to_out(*await service.save_profile(session, user, body))


@router.get("/me/resumes", response_model=ResumeList)
async def my_resumes(user: CurrentUser = Depends(candidate_only), session: AsyncSession = Depends(get_session)):
    return ResumeList(items=[ResumeOut.model_validate(r) for r in await service.list_resumes(session, user)])


@router.post("/me/resumes", status_code=status.HTTP_202_ACCEPTED, response_model=ResumeOut)
async def upload_resume(
    request: Request,
    file: UploadFile = File(...),  # multipart/form-data field named "file"
    user: CurrentUser = Depends(candidate_only),
    session: AsyncSession = Depends(get_session),
):
    # 202 Accepted: stored, but parsing (Phase 10) happens after we answer
    max_bytes = request.app.state.settings.max_resume_bytes
    data = await file.read(max_bytes + 1)  # one byte over the limit is enough to know it's too big
    resume = await service.upload_resume(session, user, file_name=file.filename, data=data, max_bytes=max_bytes)
    return ResumeOut.model_validate(resume)


@router.get("/{candidate_id}", response_model=ProfileOut)
async def get_candidate(
    candidate_id: uuid.UUID, user: CurrentUser = Depends(get_current_user), session: AsyncSession = Depends(get_session)
):
    return to_out(*await service.get_profile_for(session, user, candidate_id))
