"""Appendix E.8. notification_key UNIQUE makes every email idempotent at the database level: the same
event can't queue the same email twice, however often it is delivered."""

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from smarthire_common.models import Base, new_id

STATUSES = ("QUEUED", "SENT", "FAILED")


class NotificationTemplate(Base):
    __tablename__ = "notification_templates"

    code: Mapped[str] = mapped_column(Text, primary_key=True)  # APPLICATION_RECEIVED, STAGE_CHANGED, ...
    subject: Mapped[str] = mapped_column(Text)
    body: Mapped[str] = mapped_column(Text)  # {placeholders} filled from the log row's payload
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class NotificationLog(Base):
    __tablename__ = "notification_log"
    __table_args__ = (
        CheckConstraint("status IN ('QUEUED', 'SENT', 'FAILED')", name="status"),
        CheckConstraint("attempts >= 0", name="attempts_non_negative"),
        # The sweeper's query: WHERE status = 'QUEUED' AND created_at < ? (stuck emails)
        Index("ix_notification_log_status_created", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    notification_key: Mapped[str] = mapped_column(Text, unique=True)  # "<event_id>:<template code>"
    recipient: Mapped[str] = mapped_column(Text)  # the email address, looked up when queued
    template_code: Mapped[str] = mapped_column(Text, ForeignKey("notification_templates.code"))
    payload: Mapped[dict] = mapped_column(JSONB)  # the template's variables
    status: Mapped[str] = mapped_column(Text, server_default=text("'QUEUED'"))
    attempts: Mapped[int] = mapped_column(server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
