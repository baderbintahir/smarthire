"""All SQL for jobs and their stages. Services call these; routers never do."""

import base64
import json
import uuid
from datetime import datetime

from sqlalchemy import delete, func, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Job, JobStage


async def add(session: AsyncSession, job: Job, stage_names: list[str]) -> None:
    session.add(job)
    await session.flush()  # the stages' foreign key needs the jobs row first
    session.add_all(JobStage(job_id=job.id, name=name, position=i) for i, name in enumerate(stage_names))
    await session.flush()


async def get(session: AsyncSession, job_id: uuid.UUID) -> Job | None:
    return await session.get(Job, job_id)


async def stages_for(session: AsyncSession, job_ids: list[uuid.UUID]) -> dict[uuid.UUID, list[JobStage]]:
    """One query for the stages of many jobs (not one query per job: the "N+1" problem)."""
    result: dict[uuid.UUID, list[JobStage]] = {job_id: [] for job_id in job_ids}
    if job_ids:
        rows = await session.execute(
            select(JobStage).where(JobStage.job_id.in_(job_ids)).order_by(JobStage.job_id, JobStage.position)
        )
        for stage in rows.scalars():
            result[stage.job_id].append(stage)
    return result


# ---------- keyset pagination ----------
# "Give me 20 jobs older than the last one I saw" (WHERE (created_at, id) < cursor) instead of
# OFFSET 400: the index jumps straight to the cursor, and new jobs arriving meanwhile don't shift
# the pages. `id` breaks ties between jobs created in the same microsecond.


def encode_cursor(job: Job) -> str:
    raw = json.dumps([job.created_at.isoformat(), str(job.id)]).encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    created_at, job_id = json.loads(base64.urlsafe_b64decode(cursor.encode()))
    return datetime.fromisoformat(created_at), uuid.UUID(job_id)


async def _page(session: AsyncSession, stmt, *, limit: int, cursor: str | None) -> tuple[list[Job], str | None]:
    if cursor:
        stmt = stmt.where(tuple_(Job.created_at, Job.id) < decode_cursor(cursor))
    # limit + 1: if the extra row exists, there is a next page
    rows = list((await session.execute(stmt.order_by(Job.created_at.desc(), Job.id.desc()).limit(limit + 1))).scalars())
    page, has_more = rows[:limit], len(rows) > limit
    return page, encode_cursor(page[-1]) if has_more else None


async def list_ready(
    session: AsyncSession, *, limit: int, cursor: str | None, employment_type: str | None
) -> tuple[list[Job], str | None]:
    # status = 'READY' + ORDER BY created_at DESC: served by the partial index ix_jobs_ready_created
    stmt = select(Job).where(Job.status == "READY")
    if employment_type:
        stmt = stmt.where(Job.employment_type == employment_type)
    return await _page(session, stmt, limit=limit, cursor=cursor)


async def list_for_recruiter(
    session: AsyncSession, recruiter_id: uuid.UUID, *, status: str | None, limit: int, cursor: str | None
) -> tuple[list[Job], str | None]:
    # recruiter_id (+ status) + ORDER BY created_at DESC: served by ix_jobs_recruiter_status_created
    stmt = select(Job).where(Job.recruiter_id == recruiter_id)
    if status:
        stmt = stmt.where(Job.status == status)
    return await _page(session, stmt, limit=limit, cursor=cursor)


# ---------- conditional writes: the WHERE clause is the safety check ----------


async def update_if_version(session: AsyncSession, job_id: uuid.UUID, expected_version: int, **values) -> bool:
    """Optimistic lock: UPDATE ... WHERE id = :id AND version = :expected, and bump the version.
    0 rows updated = someone else saved in between -> False (the caller turns it into a 409).
    Nothing is locked while the user edits; the check happens in the single UPDATE statement."""
    result = await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.version == expected_version)
        .values(**values, version=Job.version + 1, updated_at=func.now())
    )
    return result.rowcount == 1


async def set_status_if(session: AsyncSession, job_id: uuid.UUID, *, expected: str, new: str) -> bool:
    """Moves the status only if it is still what the caller checked: a concurrent publish/close
    can't both succeed. False = the status changed meanwhile."""
    result = await session.execute(
        update(Job).where(Job.id == job_id, Job.status == expected).values(status=new, updated_at=func.now())
    )
    return result.rowcount == 1


async def replace_stages(session: AsyncSession, job_id: uuid.UUID, names: list[str]) -> None:
    # Delete + insert in the same transaction: the unique (job_id, position/name) constraints
    # never see old and new stages at the same time
    await session.execute(delete(JobStage).where(JobStage.job_id == job_id))
    session.add_all(JobStage(job_id=job_id, name=name, position=i) for i, name in enumerate(names))
    await session.flush()
