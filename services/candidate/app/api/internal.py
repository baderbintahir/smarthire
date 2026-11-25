"""Internal API: for other SERVICES, never for browsers.

The gateway only routes /api/candidates/* (rewritten to /candidates/*), so /internal/* can't be reached
from outside; inside Compose only other services can call it (and the service port is published on
127.0.0.1 only). There is no user token on these calls (a Temporal activity has no user), so access is
controlled by the network for the read-only endpoints. Endpoints that CHANGE data are workflow steps
(design v2.1): they require the orchestrator's token and an Idempotency-Key, and are safe to run twice."""

import uuid

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from smarthire_common.db import get_session, session_scope
from smarthire_common.errors import AppError, NotFoundError
from smarthire_common.internal import idempotency_key
from smarthire_common.service_auth import require_orchestrator
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.errors import CandidateNotFoundError
from app.repositories import candidates as repo
from app.schemas.candidate import ResumeOut
from app.services import candidates as service
from app.services import parsing

router = APIRouter(prefix="/internal", tags=["internal"])
COMMIT_STEP = [Depends(require_orchestrator), Depends(idempotency_key)]


class ResumeFacts(BaseModel):
    resume_id: uuid.UUID
    status: str  # UPLOADED (not parsed yet), PARSED, FAILED
    parsed_data: dict | None
    parse_error: str | None


class CandidateFacts(BaseModel):
    candidate_id: uuid.UUID
    experience_years: float | None  # what the candidate entered in their profile
    resume: ResumeFacts | None  # the ACTIVE resume


@router.get("/candidates/{candidate_id}/resume", response_model=CandidateFacts)
async def candidate_resume(candidate_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    """What Scoring needs (Phase 10): the profile's experience and the active resume's parsed data."""
    candidate = await repo.get(session, candidate_id)
    if candidate is None:
        raise CandidateNotFoundError()
    resume = await repo.active_resume(session, candidate_id)
    return CandidateFacts(
        candidate_id=candidate_id,
        experience_years=candidate.experience_years,
        resume=ResumeFacts(resume_id=resume.id, status=resume.status, parsed_data=resume.parsed_data,
                           parse_error=resume.parse_error) if resume else None,
    )


@router.put("/candidates/{candidate_id}", dependencies=COMMIT_STEP)
async def create_profile(candidate_id: uuid.UUID, session: AsyncSession = Depends(get_session)) -> dict:
    """create_candidate_profile step (RegistrationWorkflow, RoleChangeWorkflow): an empty profile with the
    user's id, unless one exists (a retried step, or a granted-revoked-granted user keeps their old one)."""
    await repo.create_empty(session, candidate_id)
    await session.commit()
    return {"candidate_id": str(candidate_id)}


class NewResume(BaseModel):
    id: uuid.UUID
    candidate_id: uuid.UUID
    object_key: str
    file_name: str
    content_type: str
    size_bytes: int


@router.post("/resumes", response_model=ResumeOut, dependencies=COMMIT_STEP)
async def register_resume(body: NewResume, session: AsyncSession = Depends(get_session)):
    """register_resume step (ResumeProcessingWorkflow's commit): the old resume inactive, this one UPLOADED."""
    return ResumeOut.model_validate(await service.register_resume(session, body.model_dump(mode="json")))


@router.post("/resumes/{resume_id}/parse", dependencies=COMMIT_STEP)
async def parse_resume(resume_id: uuid.UUID) -> dict:
    """parse_resume step, served by the candidate-parser container. An unreadable file is a 422: retrying
    can't fix it, so the workflow doesn't retry (the resume is FAILED with its parse_error)."""
    outcome = await parsing.parse_resume(resume_id)
    if outcome == "missing":
        raise NotFoundError("Resume not found", code="RESUME_NOT_FOUND")
    if outcome == "skipped":  # a repeated step: answer what the first run decided
        async with session_scope() as session:
            outcome = "parsed" if (await repo.get_resume(session, resume_id)).status == "PARSED" else "failed"
    if outcome == "failed":
        raise AppError("The resume could not be read", code="RESUME_UNREADABLE", http_status=422)
    return {"status": "PARSED"}
