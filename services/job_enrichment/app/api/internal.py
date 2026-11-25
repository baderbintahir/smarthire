"""Internal API: the orchestrator's workflow steps (design v2.1). Orchestrator token + Idempotency-Key.

    POST   /internal/structured-jobs                    build_structured_job
    DELETE /internal/structured-jobs/{job_id}/{version} delete_structured_job (compensation)"""

import uuid

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel
from smarthire_common.internal import idempotency_key
from smarthire_common.service_auth import require_orchestrator

from app.services import enrichment

router = APIRouter(prefix="/internal/structured-jobs", tags=["internal"],
                   dependencies=[Depends(require_orchestrator), Depends(idempotency_key)])


class BuildRequest(BaseModel):
    job_id: uuid.UUID
    version: int
    description: str
    required_skills: list[str]


@router.post("")
async def build_structured_job(body: BuildRequest) -> dict:
    return await enrichment.build(str(body.job_id), body.version, body.description, body.required_skills)


@router.delete("/{job_id}/{version}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_structured_job(job_id: uuid.UUID, version: int):
    await enrichment.remove(str(job_id), version)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
