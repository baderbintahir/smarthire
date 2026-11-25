"""Use cases for profiles and resumes (Steps 6.2-6.3). Each use case commits itself before
returning (see CLAUDE.md: get_session's commit runs after the response is sent)."""

import logging
import uuid

from smarthire_common.auth import CurrentUser
from smarthire_common.errors import ForbiddenError, StillProcessingError
from smarthire_common.models import new_id
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import resumes as resume_rules
from app.domain.errors import CandidateNotFoundError
from app.domain.profile import normalize_skills
from app.models import Candidate, Resume
from app.repositories import candidates as repo
from app.repositories import files
from app.schemas.candidate import ProfileIn

log = logging.getLogger(__name__)

# Profiles are created from the UserRegistered event (app/events/handlers.py), a moment after
# registration. Until it arrives (or if Kafka is down), a candidate can create theirs with PUT
NO_PROFILE = "Create your profile first (PUT /candidates/me)"


async def get_profile(session: AsyncSession, candidate_id: uuid.UUID) -> tuple[Candidate, Resume | None]:
    candidate = await repo.get(session, candidate_id)
    if candidate is None:
        raise CandidateNotFoundError()
    return candidate, await repo.active_resume(session, candidate_id)


async def get_profile_for(
    session: AsyncSession, user: CurrentUser, candidate_id: uuid.UUID
) -> tuple[Candidate, Resume | None]:
    """Who may read a profile: the candidate themselves, recruiters and admins.
    TODO(Phase 8): a recruiter only if the candidate applied to one of their jobs."""
    # Multi-role: a recruiter or admin may read others' profiles even if they also hold CANDIDATE
    if user.user_id != candidate_id and not user.has_role("RECRUITER", "ADMIN"):
        # 403 for EVERY other id, existing or not: the answer depends on the role, so it reveals
        # nothing about which ids exist
        raise ForbiddenError("Candidates can only see their own profile")
    return await get_profile(session, candidate_id)


async def save_profile(session: AsyncSession, user: CurrentUser, data: ProfileIn) -> tuple[Candidate, Resume | None]:
    # The id comes from the verified token: a candidate can only ever write their own profile
    await repo.upsert(
        session,
        user.user_id,
        headline=data.headline,
        skills=normalize_skills(data.skills),
        experience_years=data.experience_years,
        education=[entry.model_dump() for entry in data.education],
    )
    await session.commit()
    return await get_profile(session, user.user_id)


async def list_resumes(session: AsyncSession, user: CurrentUser) -> list[Resume]:
    return await repo.list_resumes(session, user.user_id)


async def upload_resume(
    session: AsyncSession, orchestrator, user: CurrentUser, *, file_name: str | None, data: bytes, max_bytes: int
) -> dict:
    """Design v2.1: the file goes to storage first, then the orchestrator's ResumeProcessingWorkflow
    registers it (its commit step is register_resume below; we wait for it) and has it parsed."""
    resume_rules.check_size(len(data), max_bytes)
    content_type = resume_rules.detect_content_type(data)  # from the bytes, not the client's header
    if await repo.get(session, user.user_id) is None:
        raise CandidateNotFoundError(NO_PROFILE)
    # End this read-only transaction: no DB connection is held while the file uploads
    await session.rollback()

    resume_id = new_id()
    key = resume_rules.object_key(user.user_id, resume_id)
    # File first, row second: a row must never point at a file that never arrived (the parser would fail)
    await files.put(key, data, content_type)
    resume = {"id": str(resume_id), "candidate_id": str(user.user_id), "object_key": key,
              "file_name": resume_rules.safe_file_name(file_name), "content_type": content_type, "size_bytes": len(data)}
    try:
        return await orchestrator.run_action("upload-resume", key={"resume_id": str(resume_id)},
                                             input={"resume": resume})
    except StillProcessingError:
        raise  # the workflow may still register it: the file must stay
    except BaseException:
        await _delete_quietly(key)  # nothing was registered (orchestrator down, or a rule said no)
        raise


async def register_resume(session: AsyncSession, resume: dict) -> Resume:
    """The commit step (POST /internal/resumes). Same id again (a retried step): the existing row."""
    resume_id = uuid.UUID(resume["id"])
    existing = await repo.get_resume(session, resume_id)
    if existing is not None:
        return existing
    candidate_id = uuid.UUID(resume["candidate_id"])
    # Lock the candidate row: two uploads by the same candidate take turns here, so the
    # "deactivate old, insert new" pair never interleaves (the second one waits, then wins)
    if await repo.get(session, candidate_id, for_update=True) is None:
        raise CandidateNotFoundError(NO_PROFILE)  # profile deleted meanwhile
    row = Resume(id=resume_id, candidate_id=candidate_id, object_key=resume["object_key"],
                 file_name=resume["file_name"], content_type=resume["content_type"], size_bytes=resume["size_bytes"])
    await repo.add_active_resume(session, row)
    await session.commit()
    await session.refresh(row)  # server defaults: status, is_active, uploaded_at
    return row


async def _delete_quietly(key: str) -> None:
    """Best effort: if this fails too, the object is an orphan (a file with no row). Harmless to
    users (nothing points at it); a cleanup job could remove such files later."""
    try:
        await files.delete(key)
    except Exception:
        log.warning("could not delete orphaned resume object", extra={"object_key": key})


LINK_SECONDS = 300  # 5 minutes: enough to click it, too short to be worth sharing around


async def resume_link(session: AsyncSession, user: CurrentUser, candidate_id: uuid.UUID) -> dict:
    """A short-lived download link to the candidate's ACTIVE resume, for the same people who may read
    the profile (the candidate, recruiters, admins). TODO(Phase 8 rule): recruiters of their applicants only."""
    await get_profile_for(session, user, candidate_id)  # same access rule as the profile (403/404)
    resume = await repo.active_resume(session, candidate_id)
    if resume is None:
        raise CandidateNotFoundError("This candidate has no resume")
    return {"resume_id": str(resume.id), "file_name": resume.file_name, "expires_in": LINK_SECONDS,
            "url": files.download_link(resume.object_key, resume.file_name, seconds=LINK_SECONDS)}
