"""Use cases for applications (Steps 8.3-8.5). Each one commits itself before returning (see
CLAUDE.md: get_session's commit runs after the response is sent)."""

import uuid
from datetime import UTC, datetime

from smarthire_common.auth import CurrentUser
from smarthire_common.events import new_event
from smarthire_common.outbox import add_outbox_event
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import applications as rules
from app.domain.errors import AlreadyAppliedError, ApplicationNotFoundError, VersionConflictError
from app.models import Application, ApplicationNote, JobReplica, StageHistory
from app.repositories import applications as repo
from app.repositories import job_replica as jobs
from app.repositories.candidate_api import CandidateClient
from app.schemas.application import application_out
from app.services import workflows
from app.services.idempotency import IdempotencyStore


def _job_facts(job: JobReplica | None) -> rules.JobFacts | None:
    if job is None:
        return None
    return rules.JobFacts(status=job.status, deadline=job.deadline, eligibility_rules=job.eligibility_rules,
                          stages=job.stages)


async def _publish(session: AsyncSession, event_type: str, application: Application, job: JobReplica,
                   payload: dict) -> None:
    """Queue an event about this application in the current transaction (key = application id:
    one application's events stay in order). job_title (optional, schema v2) is for notifications."""
    event = new_event(event_type, producer="application", payload={
        "application_id": str(application.id), "job_id": str(application.job_id),
        "candidate_id": str(application.candidate_id), "job_title": job.title, **payload,
    })
    await add_outbox_event(session, event, key=str(application.id))


# ---------------------------------------------------------------- 8.3 submit


async def submit(session: AsyncSession, user: CurrentUser, job_id: uuid.UUID, *, authorization: str,
                 candidates: CandidateClient, max_active: int) -> tuple[Application, JobReplica]:
    """Steps 3-6 of the apply flow (1, 2 and 7, idempotency and role, are in the router)."""
    # 3. The job is open: checked in OUR replica, no call to the Job service
    job = await jobs.get(session, job_id)
    rules.check_job_open(_job_facts(job), datetime.now(UTC))

    # 4. The candidate may apply: complete profile, and every eligibility rule passes
    candidate = await candidates.facts(authorization)
    rules.check_profile(candidate)
    rules.check_eligible(job.eligibility_rules, candidate)

    # 5. ONE transaction. The quota row lock makes this candidate's applies take turns, so
    #    "check the count, then insert" can't be raced by a second request in between
    try:
        active = await repo.lock_quota(session, user.user_id)
        rules.check_quota(active, max_active)
        application = Application(job_id=job_id, candidate_id=user.user_id, candidate_name=candidate.full_name.strip(),
                                  current_stage=rules.FIRST_STAGE)
        await repo.add(session, application)
        await repo.add_history(session, application.id, from_stage=None, to_stage=rules.FIRST_STAGE,
                               changed_by=user.user_id)
        await repo.change_quota(session, user.user_id, +1)
        # Starts the application workflow (Phase 10); Notification and Analytics react too
        await _publish(session, "ApplicationSubmitted", application, job, {"recruiter_id": str(job.recruiter_id)})
        await session.commit()
    except IntegrityError as exc:
        # 6. uq_applications_candidate_job: this candidate already applied (maybe a moment ago, in
        #    a parallel request that passed every check above too). The database is the final guard
        await session.rollback()
        if "uq_applications_candidate_job" in str(exc):
            raise AlreadyAppliedError() from None
        raise
    await session.refresh(application)
    return application, job


# ---------------------------------------------------------------- 8.4 views


async def mine(session: AsyncSession, user: CurrentUser, *, limit: int, cursor: str | None):
    return await repo.for_candidate(session, user.user_id, limit=limit, cursor=cursor)


async def board(session: AsyncSession, user: CurrentUser, job_id: uuid.UUID, *, stage: str | None, limit: int,
                offset: int) -> tuple[list[Application], JobReplica]:
    # Ownership comes from OUR replica's recruiter_id. Not your job (or unknown): 404, ids aren't confirmed
    job = await jobs.owned_by(session, job_id, user.user_id)
    if job is None:
        raise ApplicationNotFoundError()
    return await repo.board(session, job_id, stage=stage, limit=limit, offset=offset), job


async def _load(session: AsyncSession, application_id: uuid.UUID, user: CurrentUser) -> tuple[Application, JobReplica, str]:
    """The application and the caller's relation to it: "candidate" (applicant) or "recruiter" (job
    owner). Anyone else gets 404: applications are private, even their existence."""
    found = await repo.get_with_job(session, application_id)
    if found is None:
        raise ApplicationNotFoundError()
    application, job = found
    if application.candidate_id == user.user_id:
        return application, job, "candidate"
    if job.recruiter_id == user.user_id:
        return application, job, "recruiter"
    raise ApplicationNotFoundError()


async def detail(session: AsyncSession, user: CurrentUser, application_id: uuid.UUID
                 ) -> tuple[Application, JobReplica, list[StageHistory], list[ApplicationNote] | None]:
    application, job, relation = await _load(session, application_id, user)
    notes = await repo.notes(session, application_id) if relation == "recruiter" else None
    return application, job, await repo.history(session, application_id), notes


# ---------------------------------------------------------------- 8.5 changes


async def _load_as_recruiter(session, application_id, user) -> tuple[Application, JobReplica]:
    application, job, relation = await _load(session, application_id, user)
    if relation != "recruiter":
        raise ApplicationNotFoundError()  # the applicant can't act as the recruiter
    return application, job


async def _write(session: AsyncSession, application: Application, version: int | None, **values) -> None:
    application_id = application.id  # plain copy: after a rollback the ORM object is stale (MissingGreenlet)
    if not await repo.update_if(session, application_id, version=version, **values):
        await session.rollback()
        # Re-read to tell the two causes apart: it's no longer active, or someone edited it
        fresh = await repo.get_with_job(session, application_id)
        rules.check_active(fresh[0].status)
        raise VersionConflictError()


async def change_stage(session: AsyncSession, user: CurrentUser, application_id: uuid.UUID, *, to_stage: str,
                       version: int, reason: str | None) -> tuple[Application, JobReplica]:
    application, job = await _load_as_recruiter(session, application_id, user)
    rules.check_stage_change(application.status, application.current_stage, to_stage, job.stages)
    from_stage = application.current_stage
    await _write(session, application, version, current_stage=to_stage, status="IN_PROGRESS")
    await repo.add_history(session, application.id, from_stage=from_stage, to_stage=to_stage, changed_by=user.user_id,
                           reason=reason)
    await repo.audit(session, actor_id=user.user_id, action="STAGE_CHANGED", entity_id=application.id,
                     before={"stage": from_stage}, after={"stage": to_stage})
    await _publish(session, "ApplicationStageChanged", application, job, {"from_stage": from_stage, "to_stage": to_stage})
    await session.commit()
    await session.refresh(application)
    return application, job


async def decide(session: AsyncSession, user: CurrentUser, application_id: uuid.UUID, *, decision: str,
                 version: int, reason: str | None) -> tuple[Application, JobReplica]:
    application, job = await _load_as_recruiter(session, application_id, user)
    rules.check_decision(application.status, decision)
    before = {"status": application.status, "stage": application.current_stage}
    # The conditional UPDATE (only if still ACTIVE) means the quota is released exactly once, even
    # if a decision and a withdrawal arrive at the same moment
    await _write(session, application, version, status=decision)
    await repo.add_history(session, application.id, from_stage=application.current_stage, to_stage=decision,
                           changed_by=user.user_id, reason=reason)
    await repo.change_quota(session, application.candidate_id, -1)  # no longer open
    await repo.audit(session, actor_id=user.user_id, action="DECISION_MADE", entity_id=application.id,
                     before=before, after={"status": decision})
    await _publish(session, "ApplicationDecisionMade", application, job, {"decision": decision})
    await session.commit()
    await session.refresh(application)
    return application, job


async def add_note(session: AsyncSession, user: CurrentUser, application_id: uuid.UUID, body: str) -> ApplicationNote:
    application, _ = await _load_as_recruiter(session, application_id, user)
    note = ApplicationNote(application_id=application.id, recruiter_id=user.user_id, body=body)
    await repo.add_note(session, note)
    # The audit trail records that a note was added, not its text (the note itself holds that)
    await repo.audit(session, actor_id=user.user_id, action="NOTE_ADDED", entity_id=application.id,
                     before=None, after={"note_id": str(note.id)})
    await session.commit()
    await session.refresh(note)
    return note


async def withdraw(session: AsyncSession, user: CurrentUser, application_id: uuid.UUID, *,
                   settings=None) -> tuple[Application, JobReplica]:
    application, job, relation = await _load(session, application_id, user)
    if relation != "candidate":
        raise ApplicationNotFoundError()  # only the applicant withdraws
    rules.check_active(application.status)
    await _write(session, application, None, status="WITHDRAWN")
    await repo.add_history(session, application.id, from_stage=application.current_stage, to_stage="WITHDRAWN",
                           changed_by=user.user_id)
    await repo.change_quota(session, user.user_id, -1)  # frees a slot for another application
    # TODO: no event for withdrawals in the catalog yet (Notification/Analytics may want one later)
    await session.commit()
    await session.refresh(application)
    if settings is not None:  # after the commit: stop the application's workflow (best effort)
        await workflows.signal(settings, str(application.id), "withdraw")
    return application, job


async def apply(session: AsyncSession, user: CurrentUser, job_id: uuid.UUID, *, idempotency_key: str,
                idempotency: IdempotencyStore, authorization: str, candidates: CandidateClient,
                max_active: int) -> dict:
    """The whole apply flow: idempotency around submit(). Returns the response body (cached or new)."""
    fingerprint = str(job_id)  # what the key was used for: the same key for another job is an error
    # 1. Seen this key? Then this is a retry: answer exactly as the first time
    if (cached := await idempotency.begin(user.user_id, idempotency_key, fingerprint)) is not None:
        return cached
    try:
        application, job = await submit(session, user, job_id, authorization=authorization, candidates=candidates,
                                        max_active=max_active)
    except BaseException:
        await idempotency.release(user.user_id, idempotency_key)  # failed: the key may be used again
        raise
    body = application_out(application, job).model_dump(mode="json")
    # 7. Remember the answer for 24 h, then return it
    await idempotency.complete(user.user_id, idempotency_key, fingerprint, body)
    return body
