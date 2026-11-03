"""Events -> projections (Step 12.2). Each handler runs inside the consumer's transaction, right after
the event id was recorded in processed_events: the projection update and "this event is done" commit
together, so a replayed event changes nothing.

Times come from the EVENT (occurred_at), never from now(): replaying the topics a month later must
put every application on the same day as the first time (the rebuild must give identical numbers)."""

import uuid

from smarthire_common.events import EventEnvelope
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import projections as p

CONSUMER_NAME = "analytics"
TOPICS = ["identity.users", "jobs.lifecycle", "applications.lifecycle", "scoring.results", "system.failures",
          "assistant.usage"]

ROLE_COUNTERS = {"CANDIDATE": "total_candidates", "RECRUITER": "total_recruiters"}  # admins aren't counted


def is_interview_stage(stage: str) -> bool:
    """Recruiters name their own stages; "an interview stage" = any stage whose name mentions interview."""
    return "interview" in stage.lower()


async def on_user_registered(session: AsyncSession, event: EventEnvelope) -> None:
    if counter := ROLE_COUNTERS.get(event.payload["role"]):
        await p.bump_counter(session, counter)


async def on_job_ready(session: AsyncSession, event: EventEnvelope) -> None:
    job_id = uuid.UUID(event.payload["job_id"])
    await p.set_job_title(session, job_id, event.payload["title"])
    if await p.first_publish(session, job_id, event.occurred_at):  # "first version only"
        await p.bump_counter(session, "total_jobs_published")


async def on_job_publish_failed(session: AsyncSession, event: EventEnvelope) -> None:
    await p.bump_failures(session, event.occurred_at.date(), "job_publishing_workflow")


async def on_application_submitted(session: AsyncSession, event: EventEnvelope) -> None:
    payload = event.payload
    application_id, job_id = uuid.UUID(payload["application_id"]), uuid.UUID(payload["job_id"])
    candidate_id = uuid.UUID(payload["candidate_id"])
    if not await p.add_to_timeline(session, application_id, job_id, candidate_id, event.occurred_at):
        return  # already counted (a second guard, besides processed_events)
    await p.bump_counter(session, "total_applications")
    await p.bump_applications_on(session, event.occurred_at.date())
    await p.bump_job(session, job_id, payload.get("job_title") or "")
    await p.bump_candidate(session, candidate_id)


async def on_stage_changed(session: AsyncSession, event: EventEnvelope) -> None:
    if is_interview_stage(event.payload["to_stage"]):
        await p.set_timeline_once(session, uuid.UUID(event.payload["application_id"]), "interview_at", event.occurred_at)


async def on_decision(session: AsyncSession, event: EventEnvelope) -> None:
    if event.payload["decision"] == "HIRED":
        await p.set_timeline_once(session, uuid.UUID(event.payload["application_id"]), "hired_at", event.occurred_at)
        await p.bump_counter(session, "total_hires")


async def on_candidate_scored(session: AsyncSession, event: EventEnvelope) -> None:
    await p.bump_counter(session, "total_scored")


async def on_system_failure(session: AsyncSession, event: EventEnvelope) -> None:
    await p.bump_failures(session, event.occurred_at.date(), event.payload["source"])


async def on_assistant_query(session: AsyncSession, event: EventEnvelope) -> None:
    kind = "SUMMARY" if "summar" in event.payload["intent"].lower() else "QUERY"
    await p.bump_ai_usage(session, event.occurred_at.date(), event.payload["role"], kind)


HANDLERS = {
    "UserRegistered": on_user_registered,
    "JobReady": on_job_ready,
    "JobPublishFailed": on_job_publish_failed,
    "ApplicationSubmitted": on_application_submitted,
    "ApplicationStageChanged": on_stage_changed,
    "ApplicationDecisionMade": on_decision,
    "CandidateScored": on_candidate_scored,
    "SystemFailure": on_system_failure,
    "AssistantQueryCompleted": on_assistant_query,
}
