"""Internal API: the orchestrator's workflow steps on jobs (design v2.1). Orchestrator token and an
Idempotency-Key required; the gateway only routes /api/jobs/* (rewritten to /jobs/*), never /internal.

    POST /internal/jobs/{id}/processing   request_publish / edit_ready_job: JobPublishingWorkflow's commit
    POST /internal/jobs/{id}/ready        mark_ready
    POST /internal/jobs/{id}/failed       mark_failed
    POST /internal/jobs/{id}/close        close_job: JobClosingWorkflow's commit"""

import uuid

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from smarthire_common.db import get_session
from smarthire_common.internal import idempotency_key
from smarthire_common.service_auth import require_orchestrator
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import publishing

router = APIRouter(prefix="/internal/jobs", tags=["internal"],
                   dependencies=[Depends(require_orchestrator), Depends(idempotency_key)])


class StartProcessing(BaseModel):
    expected_version: int
    content: dict | None = None  # only for the edit of a READY job (already validated by the public API)
    stages: list[str] | None = None


class AtVersion(BaseModel):
    version: int


class Close(BaseModel):
    expected_version: int


@router.post("/{job_id}/processing")
async def start_processing(job_id: uuid.UUID, body: StartProcessing, session: AsyncSession = Depends(get_session)):
    return await publishing.start_processing(session, job_id, body.expected_version, content=body.content,
                                             stages=body.stages)


@router.post("/{job_id}/ready")
async def mark_ready(job_id: uuid.UUID, body: AtVersion, session: AsyncSession = Depends(get_session)):
    return await publishing.finish(session, job_id, body.version, "READY")


@router.post("/{job_id}/failed")
async def mark_failed(job_id: uuid.UUID, body: AtVersion, session: AsyncSession = Depends(get_session)):
    return await publishing.finish(session, job_id, body.version, "FAILED")


@router.post("/{job_id}/close")
async def close(job_id: uuid.UUID, body: Close, session: AsyncSession = Depends(get_session)):
    return await publishing.close(session, job_id, body.expected_version)
