"""Internal API: the orchestrator's workflow steps (design v2.1). Orchestrator token + Idempotency-Key;
the gateway only routes /api/applications/* and /api/jobs/{id}/applications here, never /internal.

    PUT  /internal/job-replicas/{job_id}         sync_job_replica (JobPublishingWorkflow)
    POST /internal/job-replicas/{job_id}/close   close_job_applications (JobClosingWorkflow)"""

import uuid

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from smarthire_common.db import get_session
from smarthire_common.internal import idempotency_key
from smarthire_common.service_auth import require_orchestrator
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import replica

router = APIRouter(prefix="/internal", tags=["internal"],
                   dependencies=[Depends(require_orchestrator), Depends(idempotency_key)])


class AtVersion(BaseModel):
    version: int


@router.put("/job-replicas/{job_id}")
async def sync_job_replica(job_id: uuid.UUID, snapshot: dict, session: AsyncSession = Depends(get_session)) -> dict:
    return {"updated": await replica.sync(session, {**snapshot, "job_id": str(job_id)})}


@router.post("/job-replicas/{job_id}/close")
async def close_job_applications(job_id: uuid.UUID, body: AtVersion,
                                 session: AsyncSession = Depends(get_session)) -> dict:
    return {"closed": await replica.close_job(session, job_id, body.version)}
