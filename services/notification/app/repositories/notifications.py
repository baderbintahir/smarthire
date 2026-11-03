"""All SQL for the notification log and templates."""

import uuid
from datetime import datetime

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import NotificationLog, NotificationTemplate


async def queue(session: AsyncSession, *, key: str, recipient: str, template_code: str, payload: dict) -> uuid.UUID | None:
    """Insert a QUEUED email unless this key exists. The id when new, None for a duplicate."""
    stmt = (insert(NotificationLog)
            .values(id=uuid.uuid4(), notification_key=key, recipient=recipient, template_code=template_code, payload=payload)
            .on_conflict_do_nothing(index_elements=[NotificationLog.notification_key])
            .returning(NotificationLog.id))
    return (await session.execute(stmt)).scalar_one_or_none()


async def lock_for_sending(session: AsyncSession, notification_id: uuid.UUID) -> tuple[NotificationLog, NotificationTemplate] | None:
    """Lock the row until commit. SKIP LOCKED: if another task is sending this very email right now,
    we get None instead of waiting, and we don't send it a second time."""
    stmt = (select(NotificationLog, NotificationTemplate).join(NotificationTemplate)
            .where(NotificationLog.id == notification_id).with_for_update(of=NotificationLog, skip_locked=True))
    row = (await session.execute(stmt)).first()
    return (row[0], row[1]) if row else None


async def exists(session: AsyncSession, notification_id: uuid.UUID) -> bool:
    return await session.get(NotificationLog, notification_id) is not None


async def mark_sent(session: AsyncSession, notification_id: uuid.UUID) -> None:
    await session.execute(update(NotificationLog).where(NotificationLog.id == notification_id)
                          .values(status="SENT", sent_at=func.now(), attempts=NotificationLog.attempts + 1, last_error=None))


async def record_failure(session: AsyncSession, notification_id: uuid.UUID, error: str, *, final: bool) -> None:
    values = {"attempts": NotificationLog.attempts + 1, "last_error": error[:1000]}
    if final:
        values["status"] = "FAILED"
    await session.execute(update(NotificationLog).where(NotificationLog.id == notification_id).values(**values))


async def stale_queued(session: AsyncSession, older_than: datetime, limit: int = 100) -> list[uuid.UUID]:
    """QUEUED emails nobody is working on (lost enqueue, broker restart). Served by
    ix_notification_log_status_created (status, created_at)."""
    stmt = (select(NotificationLog.id).where(NotificationLog.status == "QUEUED", NotificationLog.created_at < older_than)
            .order_by(NotificationLog.created_at).limit(limit))
    return list((await session.execute(stmt)).scalars())
