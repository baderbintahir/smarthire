"""Events -> queued emails (Step 11.2). Runs inside the consumer's transaction (processed_events)."""

import asyncio
import logging

import httpx
from smarthire_common.celery import make_celery
from smarthire_common.events import EventEnvelope
from smarthire_common.kafka import RetryLater
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.routing import route
from app.repositories import notifications as repo
from app.tasks.names import QUEUE, SEND_EMAIL

log = logging.getLogger(__name__)

CONSUMER_NAME = "notification"
TOPICS = ["applications.lifecycle", "jobs.lifecycle"]


def build_handlers(settings, identity) -> dict:
    producer = None  # a Celery app used only to SEND tasks (the worker container runs them)

    async def handle(session: AsyncSession, event: EventEnvelope) -> None:
        nonlocal producer
        notification = route(event)
        if notification is None:
            return
        try:
            contact = await identity.contact(notification.recipient_id)
        except httpx.HTTPError as exc:
            raise RetryLater(f"Identity unavailable: {exc}") from exc  # wait for it; don't skip the email
        if contact is None or contact["status"] != "ACTIVE":
            log.info("no email for %s: user missing or suspended", notification.recipient_id)
            return
        # Deterministic key: a redelivered event finds its row and queues nothing new
        notification_id = await repo.queue(session, key=f"{event.event_id}:{notification.template}",
                                           recipient=contact["email"], template_code=notification.template,
                                           payload=notification.variables)
        await session.commit()  # the row must exist before a worker looks for it
        if notification_id is None:
            return
        if producer is None:
            producer = make_celery("notification-api", settings.celery_broker_url, default_queue=QUEUE)
        try:
            await asyncio.to_thread(producer.send_task, SEND_EMAIL, args=[str(notification_id)], queue=QUEUE)
        except Exception:
            # The email is safely QUEUED in the database: the sweeper (requeue_stale) enqueues it later
            log.warning("could not enqueue %s now; the sweeper will", notification_id)

    return {t: handle for t in ("ApplicationSubmitted", "ApplicationStageChanged", "ApplicationDecisionMade",
                                "JobReady", "JobPublishFailed")}
