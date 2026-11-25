"""Internal API: the orchestrator's workflow steps (design v2.1). Orchestrator token + Idempotency-Key;
the gateway only routes /api/applications/* and /api/jobs/{id}/applications here, never /internal.

    PUT  /internal/job-replicas/{job_id}            sync_job_replica (JobPublishingWorkflow)
    POST /internal/job-replicas/{job_id}/close      close_job_applications (JobClosingWorkflow)
    POST /internal/applications                     submit: ApplicationWorkflow's commit
    POST /internal/applications/{id}/score          record_score_and_advance
    POST /internal/applications/{id}/stage          change_stage  } ApplicationActionWorkflow's
    POST /internal/applications/{id}/decision       decide        } commit steps
    POST /internal/applications/{id}/withdraw       withdraw      }"""

import uuid

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from smarthire_common.db import get_session
from smarthire_common.internal import idempotency_key
from smarthire_common.service_auth import require_orchestrator
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import replica, steps

router = APIRouter(prefix="/internal", tags=["internal"],
                   dependencies=[Depends(require_orchestrator), Depends(idempotency_key)])


class AtVersion(BaseModel):
    version: int


@router.put("/job-replicas/{job_id}")
async def sync_job_replica(job_id: uuid.UUID, snapshot: dict, session: AsyncSession = Depends(get_session)) -> dict:
    return {"updated": await replica.is_snapshot_applied(session, {**snapshot, "job_id": str(job_id)})}


@router.post("/job-replicas/{job_id}/close")
async def close_job_applications(job_id: uuid.UUID, body: AtVersion,
                                 session: AsyncSession = Depends(get_session)) -> dict:
    return {"closed": await replica.close_job(session, job_id, body.version)}


class NewApplication(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    candidate_id: uuid.UUID
    candidate_name: str


class Score(BaseModel):
    score: float | None  # None: scoring gave up, a recruiter scores it by hand (PENDING_MANUAL)


class StageChange(BaseModel):
    to_stage: str
    version: int
    reason: str | None = None
    actor_id: uuid.UUID


class Decision(BaseModel):
    decision: str
    version: int
    reason: str | None = None
    actor_id: uuid.UUID


@router.post("/applications")
async def submit(body: NewApplication, request: Request, session: AsyncSession = Depends(get_session)) -> dict:
    return await steps.submit(session, body.model_dump(mode="json"),
                              max_active=request.app.state.settings.max_active_applications)


@router.post("/applications/{application_id}/score")
async def record_score_and_advance(application_id: uuid.UUID, body: Score,
                                   session: AsyncSession = Depends(get_session)) -> dict:
    return await steps.record_score_and_advance(session, application_id, body.score)


@router.post("/applications/{application_id}/stage")
async def change_stage(application_id: uuid.UUID, body: StageChange, session: AsyncSession = Depends(get_session)):
    return await steps.change_stage(session, application_id, to_stage=body.to_stage, version=body.version,
                                    reason=body.reason, actor_id=body.actor_id)


@router.post("/applications/{application_id}/decision")
async def decide(application_id: uuid.UUID, body: Decision, session: AsyncSession = Depends(get_session)):
    return await steps.decide(session, application_id, decision=body.decision, version=body.version,
                              reason=body.reason, actor_id=body.actor_id)


@router.post("/applications/{application_id}/withdraw")
async def withdraw(application_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    return await steps.withdraw(session, application_id)
