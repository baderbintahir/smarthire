"""The 9 metrics (Step 12.3), computed from the read model and cached in Redis for a few seconds.

Why cache: a dashboard refreshed by many recruiters would run the same queries over and over; the
numbers only need to be fresh to within seconds (events arrive asynchronously anyway)."""

import json
from datetime import UTC, datetime, timedelta

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import metrics as q

CACHE_KEY = "metrics:dashboard"  # E.13 metrics:{name}
WINDOW_DAYS = 30


async def compute(session: AsyncSession, today=None) -> dict:
    since = (today or datetime.now(UTC).date()) - timedelta(days=WINDOW_DAYS - 1)
    counters = await q.counters(session)
    failures = await q.failures_since(session, since)
    return {
        "total_candidates": counters.get("total_candidates", 0),
        "total_recruiters": counters.get("total_recruiters", 0),
        "total_jobs_published": counters.get("total_jobs_published", 0),
        "total_applications": counters.get("total_applications", 0),
        "applications_over_time": [{"day": d.isoformat(), "count": c} for d, c in await q.applications_since(session, since)],
        "application_to_interview_rate": await q.interview_rate(session),
        "average_days_to_hire": await q.average_days_to_hire(session),
        "most_applied_jobs": await q.most_applied_jobs(session),
        "average_applications_per_candidate": await q.average_applications_per_candidate(session),
        "failures_last_30_days": {"total": sum(failures.values()), "by_source": failures},
    }


async def cached(session: AsyncSession, redis: Redis, ttl_seconds: int) -> dict:
    if (hit := await redis.get(CACHE_KEY)) is not None:
        return json.loads(hit)
    result = await compute(session)
    await redis.set(CACHE_KEY, json.dumps(result), ex=ttl_seconds)
    return result
