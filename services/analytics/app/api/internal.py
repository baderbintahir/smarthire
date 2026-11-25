"""Internal endpoint for the orchestrator (design v2.1): POST /internal/facts, the `is_new_fact_recorded` step.
Not routed by the gateway (its rewrite only reaches /analytics/*), and it requires the orchestrator's token."""

from fastapi import APIRouter, Depends
from pydantic import AwareDatetime, BaseModel, Field
from smarthire_common.db import get_session
from smarthire_common.errors import AppError
from smarthire_common.internal import idempotency_key
from smarthire_common.service_auth import require_orchestrator
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.facts import APPLIERS, is_new_fact_recorded

router = APIRouter(prefix="/internal", tags=["internal"], dependencies=[Depends(require_orchestrator)])


class FactIn(BaseModel):
    fact_id: str = Field(min_length=1, max_length=300)  # the workflow id + the fact kind
    kind: str
    occurred_at: AwareDatetime  # when it happened (the commit's time), not when the step ran
    data: dict


@router.post("/facts")
async def record(body: FactIn, _key: str = Depends(idempotency_key),
                 session: AsyncSession = Depends(get_session)) -> dict:
    if body.kind not in APPLIERS:
        raise AppError(f"Unknown fact kind {body.kind}", code="UNKNOWN_FACT_KIND", http_status=422)
    recorded = await is_new_fact_recorded(session, fact_id=body.fact_id, kind=body.kind, occurred_at=body.occurred_at,
                                 data=body.data)
    await session.commit()  # before answering: the orchestrator must not move on before it's stored
    return {"recorded": recorded}  # False = recorded before (a retried step): same outcome, nothing added

