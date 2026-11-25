"""Use cases for applications (Steps 8.3-8.5). Design v2.1: every change others react to (apply, stage,
decision, withdraw) runs as an orchestrator workflow; these functions check who may do what, answer the
friendly errors, then ask the orchestrator, whose first step is our own /internal/applications/* (steps.py).
Views and notes are plain reads and writes."""

import uuid
from datetime import UTC, datetime

from smarthire_common.auth import CurrentUser
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import applications as rules
from app.domain.errors import ApplicationNotFoundError
from app.models import Application, ApplicationNote, JobReplica, StageHistory
from app.repositories import applications as repo
from app.repositories import job_replica as jobs
from app.repositories.candidate_api import CandidateClient
from app.services.idempotency import IdempotencyStore


# Application ids from (candidate, Idempotency-Key): a retry of the SAME apply (after a 503
# STILL_PROCESSING) carries the same id and gets the first answer from the running workflow; another
# apply for the same job carries another id and gets 409 ALREADY_APPLIED. Deviation from E.1 (UUIDv7):
# uuid5 is deterministic but not time-ordered.
APPLICATION_IDS = uuid.UUID("6f1f4d0a-6c1b-5a8e-9b7e-2a4c9d0e7f11")


def application_id_for(candidate_id: uuid.UUID, idempotency_key: str) -> uuid.UUID:
    return uuid.uuid5(APPLICATION_IDS, f"{candidate_id}:{idempotency_key}")


def _job_facts(job: JobReplica | None) -> rules.JobFacts | None:
    if job is None:
        return None
    return rules.JobFacts(status=job.status, deadline=job.deadline, eligibility_rules=job.eligibility_rules,
                          stages=job.stages)


# ---------------------------------------------------------------- 8.3 submit


async def submit(session: AsyncSession, orchestrator, user: CurrentUser, job_id: uuid.UUID, *,
                 application_id: uuid.UUID, authorization: str, candidates: CandidateClient) -> dict:
    """Steps 3-6 of the apply flow (1, 2 and 7, idempotency and role, are in apply / the router)."""
    # 3. The job is open: checked in OUR replica, no call to the Job service
    job = await jobs.get(session, job_id)
    rules.check_job_open(_job_facts(job), datetime.now(UTC))
    rules.check_not_own_job(job.recruiter_id, user.user_id)  # before calling Candidate: cheap rules first

    # 4. The candidate may apply: complete profile, and every eligibility rule passes. Checked HERE: it
    #    needs the user's own token for Candidate, which never travels through the orchestrator
    candidate = await candidates.facts(authorization)
    rules.check_profile(candidate, user.name)
    rules.check_eligible(job.eligibility_rules, candidate)
    await session.rollback()  # hold no transaction while the orchestrator calls us back

    # 5-6. ApplicationWorkflow: its commit step (steps.submit) locks the quota and inserts, once
    result = await orchestrator.run_action(
        "apply", key={"job_id": str(job_id), "candidate_id": str(user.user_id)},
        input={"application": {"id": str(application_id), "job_id": str(job_id), "candidate_id": str(user.user_id),
                               "candidate_name": user.name.strip()}})
    return result["application"]


# ---------------------------------------------------------------- 8.4 views


async def mine(session: AsyncSession, user: CurrentUser, *, limit: int, cursor: str | None):
    return await repo.for_candidate(session, user.user_id, limit=limit, cursor=cursor)


async def board(session: AsyncSession, user: CurrentUser, job_id: uuid.UUID, *, stage: str | None, limit: int,
                offset: int) -> tuple[list[Application], JobReplica]:
    # Ownership comes from OUR replica's recruiter_id. Not your job (or unknown): 404, ids aren't confirmed
    job = await jobs.get_if_owned_by(session, job_id, user.user_id)
    if job is None:
        raise ApplicationNotFoundError()
    return await repo.board(session, job_id, stage=stage, limit=limit, offset=offset), job


async def stage_summary(session: AsyncSession, user: CurrentUser, job_id: uuid.UUID) -> dict:
    if await jobs.get_if_owned_by(session, job_id, user.user_id) is None:
        raise ApplicationNotFoundError()  # not your job (or unknown): 404, like the board
    by_stage = await repo.count_by_stage(session, job_id)
    return {"job_id": str(job_id), "total": sum(by_stage.values()), "by_stage": by_stage}


async def _load_visible_to(session: AsyncSession, application_id: uuid.UUID, user: CurrentUser) -> tuple[Application, JobReplica, str]:
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
    application, job, relation = await _load_visible_to(session, application_id, user)
    notes = await repo.notes(session, application_id) if relation == "recruiter" else None
    return application, job, await repo.history(session, application_id), notes


# ---------------------------------------------------------------- 8.5 changes


async def _load_as_recruiter(session, application_id, user) -> tuple[Application, JobReplica]:
    application, job, relation = await _load_visible_to(session, application_id, user)
    if relation != "recruiter":
        raise ApplicationNotFoundError()  # the applicant can't act as the recruiter
    return application, job


async def _reload(session: AsyncSession, application_id: uuid.UUID) -> tuple[Application, JobReplica]:
    session.expire_all()  # the orchestrator's step changed the row in another transaction
    return await repo.get_with_job(session, application_id)


async def change_stage(session: AsyncSession, orchestrator, user: CurrentUser, application_id: uuid.UUID, *,
                       to_stage: str, version: int, reason: str | None) -> tuple[Application, JobReplica]:
    application, job = await _load_as_recruiter(session, application_id, user)
    rules.check_stage_change(application.status, application.current_stage, to_stage, job.stages)
    await session.rollback()
    await orchestrator.run_action(
        "change-stage", key={"application_id": str(application_id), "version": version},
        input={"application_id": str(application_id), "to_stage": to_stage, "version": version, "reason": reason,
               "actor_id": str(user.user_id)})
    return await _reload(session, application_id)


async def decide(session: AsyncSession, orchestrator, user: CurrentUser, application_id: uuid.UUID, *,
                 decision: str, version: int, reason: str | None) -> tuple[Application, JobReplica]:
    application, job = await _load_as_recruiter(session, application_id, user)
    rules.check_decision(application.status, decision)
    await session.rollback()
    await orchestrator.run_action(
        "decide", key={"application_id": str(application_id)},
        input={"application_id": str(application_id), "decision": decision, "version": version, "reason": reason,
               "actor_id": str(user.user_id)})
    return await _reload(session, application_id)


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


async def withdraw(session: AsyncSession, orchestrator, user: CurrentUser,
                   application_id: uuid.UUID) -> tuple[Application, JobReplica]:
    application, job, relation = await _load_visible_to(session, application_id, user)
    if relation != "candidate":
        raise ApplicationNotFoundError()  # only the applicant withdraws
    rules.check_active(application.status)
    job_id = application.job_id  # read before the rollback (it expires ORM objects)
    await session.rollback()
    # ApplicationActionWorkflow: withdraws (quota freed), then tells the running ApplicationWorkflow to stop
    await orchestrator.run_action("withdraw", key={"application_id": str(application_id)},
                                  input={"application_id": str(application_id), "job_id": str(job_id),
                                         "candidate_id": str(user.user_id)})
    return await _reload(session, application_id)


async def apply(session: AsyncSession, orchestrator, user: CurrentUser, job_id: uuid.UUID, *, idempotency_key: str,
                idempotency: IdempotencyStore, authorization: str, candidates: CandidateClient) -> dict:
    """The whole apply flow: idempotency around submit(). Returns the response body (cached or new)."""
    fingerprint = str(job_id)  # what the key was used for: the same key for another job is an error
    # 1. Seen this key? Then this is a retry: answer exactly as the first time
    if (cached := await idempotency.begin(user.user_id, idempotency_key, fingerprint)) is not None:
        return cached
    try:
        body = await submit(session, orchestrator, user, job_id,
                            application_id=application_id_for(user.user_id, idempotency_key),
                            authorization=authorization, candidates=candidates)
    except BaseException:
        await idempotency.release(user.user_id, idempotency_key)  # failed: the key may be used again
        raise
    # 7. Remember the answer for 24 h, then return it
    await idempotency.complete(user.user_id, idempotency_key, fingerprint, body)
    return body
