"""SQL for the local job replica (fed by Job's events, Step 8.2)."""

import uuid
from datetime import datetime

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import JobReplica


async def get(session: AsyncSession, job_id: uuid.UUID) -> JobReplica | None:
    return await session.get(JobReplica, job_id)


async def upsert_if_newer(session: AsyncSession, snapshot: dict) -> bool:
    """Insert the job, or update it ONLY IF the event is newer than what we have. Events can arrive
    late or twice (replays, retries); an old snapshot must never overwrite a newer one.
    Returns False when the event was stale and nothing changed."""
    values = {
        "job_id": uuid.UUID(snapshot["job_id"]),
        "recruiter_id": uuid.UUID(snapshot["recruiter_id"]),
        "title": snapshot["title"],
        "status": snapshot["status"],
        "stages": snapshot["stages"],
        "eligibility_rules": snapshot["eligibility_rules"],
        "application_limit": snapshot["application_limit"],
        "deadline": datetime.fromisoformat(snapshot["deadline"]) if snapshot["deadline"] else None,
        "source_version": snapshot["version"],
    }
    stmt = insert(JobReplica).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=[JobReplica.job_id],
        set_={**{k: stmt.excluded[k] for k in values if k != "job_id"}, "updated_at": func.now()},
        # E.7: the version guard. Equal or older versions are ignored
        where=JobReplica.source_version < stmt.excluded.source_version,
    )
    result = await session.execute(stmt)
    return result.rowcount == 1


async def close_if_newer(session: AsyncSession, job_id: uuid.UUID, version: int) -> bool:
    """JobClosed carries no full snapshot, so it can only update a job we already know."""
    result = await session.execute(
        update(JobReplica)
        .where(JobReplica.job_id == job_id, JobReplica.source_version < version)
        .values(status="CLOSED", source_version=version, updated_at=func.now())
    )
    return result.rowcount == 1


async def owned_by(session: AsyncSession, job_id: uuid.UUID, recruiter_id: uuid.UUID) -> JobReplica | None:
    stmt = select(JobReplica).where(JobReplica.job_id == job_id, JobReplica.recruiter_id == recruiter_id)
    return (await session.execute(stmt)).scalar_one_or_none()
