"""Writes to the read model. Every write is an UPSERT (INSERT ... ON CONFLICT DO UPDATE SET count =
count + 1): no "read, add one, write back", so two events at once can't lose an increment, and a row
that doesn't exist yet is simply created."""

import uuid
from datetime import date, datetime

from sqlalchemy import case, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    AiUsageDaily,
    ApplicationsDaily,
    ApplicationTimeline,
    CandidateApplicationCount,
    FailuresDaily,
    JobApplicationCount,
    MetricCounter,
    PublishedJob,
)


async def _bump(session: AsyncSession, model, key: dict, column: str = "count", by: int = 1, **extra) -> None:
    stmt = insert(model).values(**key, **{column: by}, **extra)
    await session.execute(stmt.on_conflict_do_update(index_elements=list(key),
                                                     set_={column: getattr(model, column) + by}))


async def bump_counter(session: AsyncSession, name: str, by: int = 1) -> None:
    await _bump(session, MetricCounter, {"name": name}, column="value", by=by)


async def bump_applications_on(session: AsyncSession, day: date) -> None:
    await _bump(session, ApplicationsDaily, {"day": day})


async def bump_candidate(session: AsyncSession, candidate_id: uuid.UUID) -> None:
    await _bump(session, CandidateApplicationCount, {"candidate_id": candidate_id})


async def bump_failures(session: AsyncSession, day: date, source: str) -> None:
    await _bump(session, FailuresDaily, {"day": day, "source": source})


async def bump_ai_usage(session: AsyncSession, day: date, role: str, kind: str) -> None:
    await _bump(session, AiUsageDaily, {"day": day, "role": role, "kind": kind})


async def bump_job(session: AsyncSession, job_id: uuid.UUID, title: str) -> None:
    """One more application for this job. The row may not exist yet (its JobReady not processed yet):
    then it starts with the title from the application event, or '' until JobReady sets it."""
    stmt = insert(JobApplicationCount).values(job_id=job_id, title=title, count=1)
    await session.execute(stmt.on_conflict_do_update(
        index_elements=["job_id"],
        set_={"count": JobApplicationCount.count + 1,
              "title": case((JobApplicationCount.title == "", stmt.excluded.title), else_=JobApplicationCount.title)},
    ))


async def set_job_title(session: AsyncSession, job_id: uuid.UUID, title: str) -> None:
    stmt = insert(JobApplicationCount).values(job_id=job_id, title=title, count=0)
    await session.execute(stmt.on_conflict_do_update(index_elements=["job_id"], set_={"title": title}))


async def first_publish(session: AsyncSession, job_id: uuid.UUID, at: datetime) -> bool:
    """True only the first time this job is published (later JobReady events, after edits, don't count)."""
    stmt = insert(PublishedJob).values(job_id=job_id, first_published_at=at).on_conflict_do_nothing().returning(PublishedJob.job_id)
    return (await session.execute(stmt)).first() is not None


async def add_to_timeline(session: AsyncSession, application_id: uuid.UUID, job_id: uuid.UUID, candidate_id: uuid.UUID,
                          submitted_at: datetime) -> bool:
    """True when the application is new to the read model (so counts are bumped once per application)."""
    stmt = (insert(ApplicationTimeline)
            .values(application_id=application_id, job_id=job_id, candidate_id=candidate_id, submitted_at=submitted_at)
            .on_conflict_do_nothing().returning(ApplicationTimeline.application_id))
    return (await session.execute(stmt)).first() is not None


async def set_timeline_once(session: AsyncSession, application_id: uuid.UUID, column: str, at: datetime) -> None:
    """Record the FIRST time an application reached an interview / was hired (later events don't move it)."""
    field = getattr(ApplicationTimeline, column)
    await session.execute(update(ApplicationTimeline)
                          .where(ApplicationTimeline.application_id == application_id, field.is_(None)).values({column: at}))
