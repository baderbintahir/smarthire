"""Application's own activities for ApplicationWorkflow (task queue `application-tq`). They touch
Application's DB, so they run in Application's worker. Idempotent: a second run changes nothing more."""

import uuid
from decimal import Decimal

from smarthire_common.db import session_scope
from smarthire_common.events import new_event
from smarthire_common.outbox import add_outbox_event
from sqlalchemy import select
from temporalio import activity

from app.domain.applications import ACTIVE
from app.models import Application, JobReplica
from app.repositories import applications as repo

SYSTEM_ACTOR = uuid.UUID(int=0)  # changed_by for steps the workflow takes (not a person)
SCREENING = "SCREENING"


@activity.defn(name="init_stages")
async def init_stages(application_id: str) -> dict:
    """What the workflow needs to start: who applied to which job, and the job's requirements."""
    async with session_scope() as session:
        found = await repo.get_with_job(session, uuid.UUID(application_id))
        if found is None:
            return {"active": False}
        application, job = found
        return {
            "active": application.status in ACTIVE,
            "candidate_id": str(application.candidate_id),
            "job_id": str(application.job_id),
            "min_experience_years": job.eligibility_rules.get("min_experience_years"),
        }


@activity.defn(name="record_score_and_advance")
async def record_score_and_advance(application_id: str, result: dict | None, failure: str | None = None) -> str:
    """Save the score (or PENDING_MANUAL), and move a still-SUBMITTED application to SCREENING with its
    history row and ApplicationStageChanged, all in one transaction. If a recruiter already moved it,
    only the score is saved; if it's no longer active (withdrawn, closed, decided), nothing happens.
    `failure` (scoring gave up) is reported as a SystemFailure event in the same transaction."""
    async with session_scope() as session:
        stmt = (select(Application, JobReplica).join(JobReplica)
                .where(Application.id == uuid.UUID(application_id)).with_for_update(of=Application))
        row = (await session.execute(stmt)).first()
        if row is None or row[0].status not in ACTIVE:
            return "SKIPPED"
        application, job = row
        values = {"score": Decimal(str(result["score"])), "score_status": "SCORED"} if result else {"score_status": "PENDING_MANUAL"}
        advance = application.status == "SUBMITTED"
        if advance:
            values.update(status=SCREENING, current_stage=SCREENING)
        from_stage = application.current_stage
        await repo.update_if(session, application.id, **values)
        if advance:
            await repo.add_history(session, application.id, from_stage=from_stage, to_stage=SCREENING,
                                   changed_by=SYSTEM_ACTOR, reason="scored" if result else "score pending (manual)")
            event = new_event("ApplicationStageChanged", producer="application", payload={
                "application_id": application_id, "job_id": str(application.job_id),
                "candidate_id": str(application.candidate_id), "job_title": job.title,
                "from_stage": from_stage, "to_stage": SCREENING})
            await add_outbox_event(session, event, key=application_id)
        if failure:  # operators see it in Analytics' failure counts; the application still moves on
            report = new_event("SystemFailure", producer="application", payload={
                "source": "application_workflow", "kind": "WORKFLOW_FAILED",
                "detail": f"application {application_id}: {failure}"[:1000]})
            await add_outbox_event(session, report, key=application_id)
        return SCREENING if advance else "SCORE_ONLY"


ACTIVITIES = [init_stages, record_score_and_advance]
