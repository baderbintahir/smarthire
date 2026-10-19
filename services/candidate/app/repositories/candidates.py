"""All SQL for candidates and resumes. Services call these; routers never do."""

import uuid

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Candidate, Resume


async def get(session: AsyncSession, candidate_id: uuid.UUID, *, for_update: bool = False) -> Candidate | None:
    stmt = select(Candidate).where(Candidate.id == candidate_id)
    if for_update:
        # Row lock until commit: a second transaction asking for the same row waits here
        stmt = stmt.with_for_update()
    return (await session.execute(stmt)).scalar_one_or_none()


async def upsert(session: AsyncSession, candidate_id: uuid.UUID, **values) -> None:
    """INSERT, or UPDATE if the row exists, in ONE statement. "Check, then insert" as two steps
    would race: two first saves at once would both see "no row", and the second insert would fail."""
    stmt = insert(Candidate).values(id=candidate_id, **values)
    await session.execute(
        stmt.on_conflict_do_update(index_elements=[Candidate.id], set_={**values, "updated_at": func.now()})
    )


async def active_resume(session: AsyncSession, candidate_id: uuid.UUID) -> Resume | None:
    # Served by the partial unique index uq_resumes_one_active (it holds active rows only)
    stmt = select(Resume).where(Resume.candidate_id == candidate_id, Resume.is_active)
    return (await session.execute(stmt)).scalar_one_or_none()


async def list_resumes(session: AsyncSession, candidate_id: uuid.UUID, *, limit: int = 50) -> list[Resume]:
    # Served by ix_resumes_candidate_uploaded (candidate_id, uploaded_at DESC)
    stmt = (
        select(Resume)
        .where(Resume.candidate_id == candidate_id)
        .order_by(Resume.uploaded_at.desc(), Resume.id.desc())
        .limit(limit)
    )
    return list((await session.execute(stmt)).scalars())


async def add_active_resume(session: AsyncSession, resume: Resume) -> None:
    """Deactivate the current resume, THEN insert the new active one. The order matters: the
    partial unique index allows one active row per candidate at every moment, even mid-transaction."""
    await session.execute(
        update(Resume).where(Resume.candidate_id == resume.candidate_id, Resume.is_active).values(is_active=False)
    )
    session.add(resume)
    await session.flush()
