"""Facts -> projections (design v2.0; the Step 12.2 handlers, now fed by facts instead of Kafka events).

record_fact stores the fact and, only if it is new, applies it to the projections, in the caller's
transaction: "stored" and "counted" commit together, so recording the same fact twice changes nothing.

Times come from the FACT (occurred_at), never from now(): a rebuild a month later must put every
application on the same day as the first time."""

import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import facts as facts_repo
from app.repositories import projections as p

# Totals count ROLE HOLDERS (design v1.1): a recruiter who also holds CANDIDATE is in both. Admins aren't counted.
ROLE_COUNTERS = {"CANDIDATE": "total_candidates", "RECRUITER": "total_recruiters"}

# What a fact keeps: the fields the appliers read, nothing else (no personal data, small rows)
FACT_FIELDS = {
    "UserRegistered": ("user_id", "role", "roles"),
    "RoleGranted": ("user_id", "role"),
    "RoleRevoked": ("user_id", "role"),
    "JobReady": ("job_id", "title", "version"),
    "JobPublishFailed": ("job_id", "version"),
    "ApplicationSubmitted": ("application_id", "job_id", "candidate_id", "job_title"),
    "ApplicationStageChanged": ("application_id", "to_stage"),
    "ApplicationDecisionMade": ("application_id", "decision"),
    "CandidateScored": ("application_id",),
    "SystemFailure": ("source", "kind"),
    "AssistantQueryCompleted": ("role", "intent"),
}


def is_interview_stage(stage: str) -> bool:
    """Recruiters name their own stages; "an interview stage" = any stage whose name mentions interview."""
    return "interview" in stage.lower()


def slim(kind: str, data: dict) -> dict:
    return {k: data[k] for k in FACT_FIELDS[kind] if k in data}


async def _user_registered(session: AsyncSession, data: dict, at: datetime) -> None:
    # `roles` arrived in schema v2; facts copied from v1 events only have the single `role`
    for role in data.get("roles") or [data["role"]]:
        if counter := ROLE_COUNTERS.get(role):
            await p.bump_counter(session, counter)


async def _role_granted(session: AsyncSession, data: dict, at: datetime) -> None:
    if counter := ROLE_COUNTERS.get(data["role"]):
        await p.bump_counter(session, counter)


async def _role_revoked(session: AsyncSession, data: dict, at: datetime) -> None:
    if counter := ROLE_COUNTERS.get(data["role"]):
        await p.drop_counter(session, counter)  # never below 0 (users from before events were never counted)


async def _job_ready(session: AsyncSession, data: dict, at: datetime) -> None:
    job_id = uuid.UUID(data["job_id"])
    await p.set_job_title(session, job_id, data["title"])
    if await p.first_publish(session, job_id, at):  # "first version only"
        await p.bump_counter(session, "total_jobs_published")


async def _job_publish_failed(session: AsyncSession, data: dict, at: datetime) -> None:
    await p.bump_failures(session, at.date(), "job_publishing_workflow")


async def _application_submitted(session: AsyncSession, data: dict, at: datetime) -> None:
    application_id, job_id = uuid.UUID(data["application_id"]), uuid.UUID(data["job_id"])
    candidate_id = uuid.UUID(data["candidate_id"])
    if not await p.add_to_timeline(session, application_id, job_id, candidate_id, at):
        return  # already counted (a second guard, besides the fact id)
    await p.bump_counter(session, "total_applications")
    await p.bump_applications_on(session, at.date())
    await p.bump_job(session, job_id, data.get("job_title") or "")
    await p.bump_candidate(session, candidate_id)


async def _stage_changed(session: AsyncSession, data: dict, at: datetime) -> None:
    if is_interview_stage(data["to_stage"]):
        await p.set_timeline_once(session, uuid.UUID(data["application_id"]), "interview_at", at)


async def _decision(session: AsyncSession, data: dict, at: datetime) -> None:
    if data["decision"] == "HIRED":
        await p.set_timeline_once(session, uuid.UUID(data["application_id"]), "hired_at", at)
        await p.bump_counter(session, "total_hires")


async def _candidate_scored(session: AsyncSession, data: dict, at: datetime) -> None:
    await p.bump_counter(session, "total_scored")


async def _system_failure(session: AsyncSession, data: dict, at: datetime) -> None:
    await p.bump_failures(session, at.date(), data["source"])


async def _assistant_query(session: AsyncSession, data: dict, at: datetime) -> None:
    kind = "SUMMARY" if "summar" in data["intent"].lower() else "QUERY"
    await p.bump_ai_usage(session, at.date(), data["role"], kind)


APPLIERS: dict[str, Callable[[AsyncSession, dict, datetime], Awaitable[None]]] = {
    "UserRegistered": _user_registered,
    "RoleGranted": _role_granted,
    "RoleRevoked": _role_revoked,
    "JobReady": _job_ready,
    "JobPublishFailed": _job_publish_failed,
    "ApplicationSubmitted": _application_submitted,
    "ApplicationStageChanged": _stage_changed,
    "ApplicationDecisionMade": _decision,
    "CandidateScored": _candidate_scored,
    "SystemFailure": _system_failure,
    "AssistantQueryCompleted": _assistant_query,
}


async def record_fact(session: AsyncSession, *, fact_id: str, kind: str, occurred_at: datetime, data: dict) -> bool:
    """Store the fact and apply it if new. True = counted now, False = already recorded before."""
    data = slim(kind, data)
    if not await facts_repo.insert_fact(session, fact_id=fact_id, kind=kind, occurred_at=occurred_at, data=data):
        return False
    await APPLIERS[kind](session, data, occurred_at)
    return True


async def rebuild(session: AsyncSession) -> int:
    """Empty the projections and re-apply every fact in time order, in ONE transaction (the caller's)."""
    await facts_repo.lock_and_empty_projections(session)
    rows = await facts_repo.all_in_time_order(session)
    for kind, data, occurred_at in rows:
        await APPLIERS[kind](session, data, occurred_at)
    return len(rows)
