"""Celery tasks: send one email, and the sweeper that re-enqueues stuck ones (Step 11.3)."""

import logging
import uuid
from datetime import UTC, datetime, timedelta

from smarthire_common.celery import run_async
from smarthire_common.db import session_scope

from app.repositories import notifications as repo
from app.services import sending
from app.tasks.celery_app import celery_app, settings
from app.tasks.names import REQUEUE_STALE, SEND_EMAIL

log = logging.getLogger(__name__)


@celery_app.task(name=SEND_EMAIL, bind=True, max_retries=20)
def send_email(self, notification_id: str) -> str:
    mailer = sending.smtp_mailer(settings.smtp_host, settings.smtp_port, settings.smtp_from)
    try:
        return run_async(sending.send(uuid.UUID(notification_id), mailer))
    except sending.SendLater as exc:
        # Back-off: 5 s, 10 s, 20 s, ... up to 5 min between tries. The email stays QUEUED meanwhile
        raise self.retry(exc=exc, countdown=min(5 * 2 ** self.request.retries, 300)) from exc


@celery_app.task(name=REQUEUE_STALE)
def requeue_stale() -> int:
    """Safety net: emails QUEUED for over 2 minutes whose task got lost (enqueue failed, broker lost
    it). Enqueuing a duplicate is harmless: sending locks the row and only sends QUEUED ones."""
    async def find() -> list[uuid.UUID]:
        async with session_scope() as session:
            return await repo.stale_queued(session, datetime.now(UTC) - timedelta(minutes=2))

    ids = run_async(find())
    for notification_id in ids:
        send_email.delay(str(notification_id))
    if ids:
        log.info("re-enqueued %d stuck emails", len(ids))
    return len(ids)
