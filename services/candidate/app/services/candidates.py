"""Use cases for profiles and resumes (Steps 6.2-6.3). Each use case commits itself before
returning (see CLAUDE.md: get_session's commit runs after the response is sent)."""

import logging
import uuid

from smarthire_common.auth import CurrentUser
from smarthire_common.errors import ForbiddenError
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

# Until Phase 7 creates profiles from the UserRegistered event, a candidate makes their own with PUT
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
    if user.role == "CANDIDATE" and user.user_id != candidate_id:
        # 403 for EVERY other id, existing or not: the answer depends on the role, so it reveals
        # nothing about which ids exist
        raise ForbiddenError("Candidates can only see their own profile")
    return await get_profile(session, candidate_id)


async def save_profile(session: AsyncSession, user: CurrentUser, data: ProfileIn) -> tuple[Candidate, Resume | None]:
    # The id comes from the verified token: a candidate can only ever write their own profile
    await repo.upsert(
        session,
        user.user_id,
        full_name=data.full_name,
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
    session: AsyncSession, user: CurrentUser, *, file_name: str | None, data: bytes, max_bytes: int
) -> Resume:
    resume_rules.check_size(len(data), max_bytes)
    content_type = resume_rules.detect_content_type(data)  # from the bytes, not the client's header
    if await repo.get(session, user.user_id) is None:
        raise CandidateNotFoundError(NO_PROFILE)
    # End this read-only transaction: no DB connection is held while the file uploads
    await session.rollback()

    resume_id = new_id()
    key = resume_rules.object_key(user.user_id, resume_id)
    # File first, row second. If the row fails we delete the file. The other order could leave a
    # row pointing at a file that never arrived, and the parser (Phase 10) would fail on it.
    await files.put(key, data, content_type)
    try:
        # Lock the candidate row: two uploads by the same candidate take turns here, so the
        # "deactivate old, insert new" pair never interleaves (the second one waits, then wins)
        if await repo.get(session, user.user_id, for_update=True) is None:
            raise CandidateNotFoundError(NO_PROFILE)  # profile deleted while we were uploading
        resume = Resume(
            id=resume_id,
            candidate_id=user.user_id,
            object_key=key,
            file_name=resume_rules.safe_file_name(file_name),
            content_type=content_type,
            size_bytes=len(data),
        )
        await repo.add_active_resume(session, resume)
        # TODO(Phase 7.7): ResumeUploaded outbox row; TODO(Phase 10.2): enqueue parse_resume(resume_id)
        await session.commit()
    except BaseException:
        await session.rollback()
        await _delete_quietly(key)
        raise
    await session.refresh(resume)  # server defaults: status, is_active, uploaded_at
    return resume


async def _delete_quietly(key: str) -> None:
    """Best effort: if this fails too, the object is an orphan (a file with no row). Harmless to
    users (nothing points at it); a cleanup job could remove such files later."""
    try:
        await files.delete(key)
    except Exception:
        log.warning("could not delete orphaned resume object", extra={"object_key": key})
