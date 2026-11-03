"""Internal API: for other SERVICES, never for browsers.

The gateway only routes /api/candidates/* (rewritten to /candidates/*), so /internal/* can't be reached
from outside; inside Compose only other services can call it (and the service port is published on
127.0.0.1 only). There is no user token on these calls (a Temporal activity has no user), so access is
controlled by the network, for now.
TODO(Phase 14): service-to-service authentication (service tokens or mTLS) on /internal/*."""

import uuid

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from smarthire_common.db import get_session
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.errors import CandidateNotFoundError
from app.repositories import candidates as repo

router = APIRouter(prefix="/internal", tags=["internal"])


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
