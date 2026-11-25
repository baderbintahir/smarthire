"""Application's workflow steps (design v2.1): called by the orchestrator's ApplicationWorkflow and
ApplicationActionWorkflow through /internal/applications/*. Each is one transaction and safe to run twice
(Temporal repeats a step after a timeout): "already done" answers like the first time.

The public endpoints (app/services/applications.py) check who may do what and answer the friendly
errors first; these steps check the rules again inside their transaction, at the moment of the change."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

from prometheus_client import Counter
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import applications as rules
from app.domain.applications import ApplicationStatus, ScoreStatus
from app.domain.errors import AlreadyAppliedError, ApplicationNotFoundError, VersionConflictError
from app.models import Application, JobReplica
from app.repositories import applications as repo
from app.repositories import job_replica as jobs
from app.schemas.application import application_out

SYSTEM_ACTOR = uuid.UUID(int=0)  # changed_by for steps the workflow takes (not a person)
SCREENING = "SCREENING"

# A business metric (Step 13.4): Prometheus shows applications per minute without asking Analytics
APPLICATIONS_SUBMITTED = Counter("smarthire_applications_submitted_total", "Applications submitted")


def _answer(application: Application, job: JobReplica) -> dict:
    """The application as the caller sees it, plus what the workflow needs for its later steps."""
    return {"application": application_out(application, job).model_dump(mode="json"),
            "context": {"job_title": job.title, "recruiter_id": str(job.recruiter_id),
                        "min_experience_years": job.eligibility_rules.get("min_experience_years")}}


async def _found(session: AsyncSession, application_id: uuid.UUID) -> tuple[Application, JobReplica]:
    found = await repo.get_with_job(session, application_id)
    if found is None:
        raise ApplicationNotFoundError()
    return found


async def submit(session: AsyncSession, data: dict, *, max_active: int) -> dict:
    """ApplicationWorkflow's commit: the job is still open, the quota allows it, then one INSERT."""
    application_id = uuid.UUID(data["id"])
    if (existing := await repo.get_with_job(session, application_id)) is not None:
        return _answer(*existing)  # a retried step
    job_id, candidate_id = uuid.UUID(data["job_id"]), uuid.UUID(data["candidate_id"])
    job = await jobs.get(session, job_id)
    rules.check_job_open(rules.JobFacts(status=job.status, deadline=job.deadline, eligibility_rules=job.eligibility_rules,
                                        stages=job.stages) if job else None, datetime.now(UTC))
    rules.check_not_own_job(job.recruiter_id, candidate_id)
    # ONE transaction. The quota row lock makes this candidate's applies take turns, so "check the
    # count, then insert" can't be raced by a second request in between
    try:
        rules.check_quota(await repo.lock_quota(session, candidate_id), max_active)
        application = Application(id=application_id, job_id=job_id, candidate_id=candidate_id,
                                  candidate_name=data["candidate_name"], current_stage=rules.FIRST_STAGE)
        await repo.add(session, application)
        await repo.add_history(session, application_id, from_stage=None, to_stage=rules.FIRST_STAGE,
                               changed_by=candidate_id)
        await repo.change_quota(session, candidate_id, +1)
        await session.commit()
    except IntegrityError as exc:
        # uq_applications_candidate_job: this candidate already applied. The database is the final guard
        await session.rollback()
        if "uq_applications_candidate_job" in str(exc):
            raise AlreadyAppliedError() from None
        raise
    APPLICATIONS_SUBMITTED.inc()  # only once committed
    return _answer(*await _found(session, application_id))


async def record_score_and_advance(session: AsyncSession, application_id: uuid.UUID, score: float | None) -> dict:
    """Save the score (or PENDING_MANUAL when scoring gave up) and move a still-SUBMITTED application to
    SCREENING. A recruiter already moved it: only the score. No longer active: nothing. A repeat finds it
    SCREENING already and doesn't advance it twice."""
    stmt = (select(Application, JobReplica).join(JobReplica)
            .where(Application.id == application_id).with_for_update(of=Application))
    row = (await session.execute(stmt)).first()
    if row is None or row[0].status not in rules.ACTIVE:
        return {"advanced": False}
    application, job = row
    values = ({"score": Decimal(str(score)), "score_status": ScoreStatus.SCORED} if score is not None
              else {"score_status": ScoreStatus.PENDING_MANUAL})
    advance = application.status == ApplicationStatus.SUBMITTED
    if advance:
        values.update(status=SCREENING, current_stage=SCREENING)
    from_stage = application.current_stage
    await repo.is_application_updated(session, application.id, **values)
    if advance:
        await repo.add_history(session, application.id, from_stage=from_stage, to_stage=SCREENING,
                               changed_by=SYSTEM_ACTOR, reason="scored" if score is not None else "score pending (manual)")
    await session.commit()
    return {"advanced": advance}


async def _update_active_or_raise(session: AsyncSession, application_id: uuid.UUID, version: int | None, **values) -> None:
    if not await repo.is_application_updated(session, application_id, version=version, **values):
        await session.rollback()
        # Re-read to tell the two causes apart: it's no longer active, or someone edited it
        fresh = await repo.get_with_job(session, application_id)
        rules.check_active(fresh[0].status)
        raise VersionConflictError()


async def change_stage(session: AsyncSession, application_id: uuid.UUID, *, to_stage: str, version: int,
                       reason: str | None, actor_id: uuid.UUID) -> dict:
    application, job = await _found(session, application_id)
    if application.version == version + 1 and application.current_stage == to_stage:
        return _answer(application, job)  # a retried step
    rules.check_stage_change(application.status, application.current_stage, to_stage, job.stages)
    from_stage = application.current_stage
    await _update_active_or_raise(session, application_id, version, current_stage=to_stage, status=ApplicationStatus.IN_PROGRESS)
    await repo.add_history(session, application_id, from_stage=from_stage, to_stage=to_stage, changed_by=actor_id,
                           reason=reason)
    await repo.audit(session, actor_id=actor_id, action="STAGE_CHANGED", entity_id=application_id,
                     before={"stage": from_stage}, after={"stage": to_stage})
    await session.commit()
    return _answer(*await _found(session, application_id))


async def decide(session: AsyncSession, application_id: uuid.UUID, *, decision: str, version: int,
                 reason: str | None, actor_id: uuid.UUID) -> dict:
    application, job = await _found(session, application_id)
    if application.status == decision:
        return _answer(application, job)  # a retried step (a decision is final: it can't be made twice)
    rules.check_decision(application.status, decision)
    before = {"status": application.status, "stage": application.current_stage}
    stage, candidate_id = application.current_stage, application.candidate_id
    # The conditional UPDATE (only if still ACTIVE) means the quota is released exactly once, even
    # if a decision and a withdrawal arrive at the same moment
    await _update_active_or_raise(session, application_id, version, status=decision)
    await repo.add_history(session, application_id, from_stage=stage, to_stage=decision, changed_by=actor_id,
                           reason=reason)
    await repo.change_quota(session, candidate_id, -1)  # no longer open
    await repo.audit(session, actor_id=actor_id, action="DECISION_MADE", entity_id=application_id,
                     before=before, after={"status": decision})
    await session.commit()
    return _answer(*await _found(session, application_id))


async def withdraw(session: AsyncSession, application_id: uuid.UUID) -> dict:
    application, job = await _found(session, application_id)
    if application.status == ApplicationStatus.WITHDRAWN:
        return _answer(application, job)  # a retried step
    rules.check_active(application.status)
    stage, candidate_id = application.current_stage, application.candidate_id
    await _update_active_or_raise(session, application_id, None, status=ApplicationStatus.WITHDRAWN)
    await repo.add_history(session, application_id, from_stage=stage, to_stage="WITHDRAWN", changed_by=candidate_id)
    await repo.change_quota(session, candidate_id, -1)  # frees a slot for another application
    await session.commit()
    return _answer(*await _found(session, application_id))
