"""Internal endpoints for SendEmailWorkflow (design v2.1). Orchestrator token required; no gateway route.

    POST /internal/emails                record_email: one QUEUED row per notification_key (idempotent)
    POST /internal/emails/{key}/send     send_email: SMTP behind the circuit breaker; 503 while it fails,
                                         so the workflow retries with backoff; SENT is recorded once
    POST /internal/emails/{key}/failed   mark_email_failed: the workflow's retries ran out"""

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from smarthire_common.db import get_session, session_scope
from smarthire_common.errors import NotFoundError, ServiceUnavailableError
from smarthire_common.internal import idempotency_key
from smarthire_common.service_auth import require_orchestrator
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import notifications as repo
from app.services import sending

router = APIRouter(prefix="/internal/emails", tags=["internal"],
                   dependencies=[Depends(require_orchestrator), Depends(idempotency_key)])


class EmailIn(BaseModel):
    notification_key: str = Field(min_length=1, max_length=300)  # the SendEmailWorkflow's id
    recipient: str = Field(min_length=3, max_length=320)
    template_code: str
    variables: dict


@router.post("")
async def record_email(body: EmailIn, session: AsyncSession = Depends(get_session)) -> dict:
    await repo.queue(session, key=body.notification_key, recipient=body.recipient,
                     template_code=body.template_code, payload=body.variables)
    await session.commit()
    log = await repo.find_by_key(session, body.notification_key)  # new or already there: same answer
    return {"notification_id": str(log.id), "status": log.status}


async def _id_of(key: str):
    async with session_scope() as session:
        log = await repo.find_by_key(session, key)
    if log is None:
        raise NotFoundError("No such email", code="EMAIL_NOT_FOUND")
    return log.id


@router.post("/{key}/send")
async def send_email(key: str, request: Request) -> dict:
    notification_id = await _id_of(key)
    try:
        outcome = await sending.send(notification_id, request.app.state.mailer)
    except sending.SendLater as exc:
        # 5xx: "try again later". The attempt and its error are recorded on the row
        raise ServiceUnavailableError(f"SMTP unavailable: {exc}", code="SMTP_UNAVAILABLE") from None
    if outcome == "busy":
        raise ServiceUnavailableError("This email is being sent right now", code="EMAIL_BUSY")
    async with session_scope() as session:
        status = (await repo.find_by_key(session, key)).status
    return {"status": status}  # SENT now, or SENT/FAILED before (a retried step)


@router.post("/{key}/failed")
async def mark_email_failed(key: str) -> dict:
    notification_id = await _id_of(key)
    async with session_scope() as session:
        await repo.mark_failed_if_queued(session, notification_id)
    async with session_scope() as session:
        return {"status": (await repo.find_by_key(session, key)).status}
