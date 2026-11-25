"""Use case: send one queued email (Step 11.3; design v2.0: the send_email step of the orchestrator's
SendEmailWorkflow, POST /internal/emails/{key}/send).

Never twice: the row is locked while sending (a parallel duplicate call skips it), and only a QUEUED
row is sent; SENT is recorded in the same transaction. Never blocking hiring: it runs in its own
workflow, far from any request; if SMTP is down, the workflow retries with backoff.

A CIRCUIT BREAKER guards SMTP: after 3 failures in a row it "opens" and, for 30 s, fails instantly
without even trying (no point hammering a server that's down, and no tasks stuck in timeouts). Then
it lets one attempt through ("half-open"); success closes it again."""

import asyncio
import smtplib
import uuid
from collections.abc import Callable
from email.message import EmailMessage

import pybreaker
from opentelemetry.trace import SpanKind
from smarthire_common.db import session_scope
from smarthire_common.telemetry import get_tracer

from app.domain.templates import render
from app.models import EmailStatus
from app.repositories import notifications as repo

breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=30)  # one per worker process


class SendLater(Exception):
    """Temporary trouble (SMTP down, circuit open): the task retries later with back-off."""


def smtp_mailer(host: str, port: int, sender: str) -> Callable[[str, str, str], None]:
    def send(to: str, subject: str, body: str) -> None:
        message = EmailMessage()
        message["From"], message["To"], message["Subject"] = sender, to, subject
        message.set_content(body)
        # smtplib has no OpenTelemetry instrumentation: a hand-made span, the last hop of the trace
        with get_tracer().start_as_current_span("smtp send", kind=SpanKind.CLIENT,
                                                attributes={"server.address": host, "server.port": port}):
            with smtplib.SMTP(host, port, timeout=10) as smtp:
                smtp.send_message(message)

    return send


async def send(notification_id: uuid.UUID, mailer: Callable[[str, str, str], None]) -> str:
    """Returns what happened: sent, skipped (already done), busy (another task has it), missing, failed.
    Never gives up by itself: SendEmailWorkflow's retry policy decides when to stop (design v2.0)."""
    async with session_scope() as session:
        locked = await repo.lock_for_sending(session, notification_id)
        if locked is None:
            return "busy" if await repo.is_email_logged(session, notification_id) else "missing"
        log, template = locked
        if log.status != EmailStatus.QUEUED:
            return "skipped"  # SENT or FAILED already: a duplicate task
        subject, body = render(template.subject, template.body, log.payload)
        try:
            # smtplib blocks: run it in a thread. The breaker counts failures across tasks in this process
            await asyncio.to_thread(breaker.call, mailer, log.recipient, subject, body)
        except pybreaker.CircuitBreakerError as exc:
            raise SendLater("circuit open: SMTP recently failing, not trying for now") from exc
        except (smtplib.SMTPException, OSError) as exc:
            await repo.record_failure(session, notification_id, f"{type(exc).__name__}: {exc}", final=False)
            await session.commit()  # keep the failure record (raising below would roll it back)
            raise SendLater(str(exc)) from exc
        await repo.mark_sent(session, notification_id)
    return "sent"
