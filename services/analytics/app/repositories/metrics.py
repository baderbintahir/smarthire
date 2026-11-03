"""Reads for the metrics API (Step 12.3): small queries on the read model, no joins across services."""

from datetime import date

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    ApplicationsDaily,
    ApplicationTimeline,
    CandidateApplicationCount,
    FailuresDaily,
    JobApplicationCount,
    MetricCounter,
)


async def counters(session: AsyncSession) -> dict[str, int]:
    return {name: value for name, value in (await session.execute(select(MetricCounter.name, MetricCounter.value))).all()}


async def applications_since(session: AsyncSession, since: date) -> list[tuple[date, int]]:
    stmt = select(ApplicationsDaily.day, ApplicationsDaily.count).where(ApplicationsDaily.day >= since).order_by(ApplicationsDaily.day)
    return [tuple(row) for row in (await session.execute(stmt)).all()]


async def interview_rate(session: AsyncSession) -> float | None:
    total, interviewed = (await session.execute(
        select(func.count(), func.count(ApplicationTimeline.interview_at)))).one()  # count(col) skips NULLs
    return round(interviewed / total, 4) if total else None


async def average_days_to_hire(session: AsyncSession) -> float | None:
    seconds = (await session.execute(select(func.avg(func.extract(
        "epoch", ApplicationTimeline.hired_at - ApplicationTimeline.submitted_at))).where(ApplicationTimeline.hired_at.is_not(None)))).scalar_one()
    return round(float(seconds) / 86400, 2) if seconds is not None else None


async def most_applied_jobs(session: AsyncSession, limit: int = 5) -> list[dict]:
    # Served by ix_job_application_counts_count (count DESC)
    stmt = (select(JobApplicationCount.job_id, JobApplicationCount.title, JobApplicationCount.count)
            .where(JobApplicationCount.count > 0).order_by(JobApplicationCount.count.desc(), JobApplicationCount.job_id).limit(limit))
    return [{"job_id": str(j), "title": t, "applications": c} for j, t, c in (await session.execute(stmt)).all()]


async def average_applications_per_candidate(session: AsyncSession) -> float | None:
    total, candidates = (await session.execute(
        select(func.coalesce(func.sum(CandidateApplicationCount.count), 0), func.count()))).one()
    return round(total / candidates, 2) if candidates else None


async def failures_since(session: AsyncSession, since: date) -> dict[str, int]:
    stmt = (select(FailuresDaily.source, func.sum(FailuresDaily.count)).where(FailuresDaily.day >= since)
            .group_by(FailuresDaily.source).order_by(FailuresDaily.source))
    return {source: int(total) for source, total in (await session.execute(stmt)).all()}
