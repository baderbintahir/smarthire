"""Appendix E.7, the core of SmartHire. Rules the database can enforce are constraints."""

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Numeric,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from smarthire_common.models import Base, TimestampMixin, new_id

STATUSES = ("SUBMITTED", "SCREENING", "IN_PROGRESS", "HIRED", "REJECTED", "WITHDRAWN", "CLOSED")
SCORE_STATUSES = ("PENDING", "SCORED", "PENDING_MANUAL")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class JobReplica(Base):
    """A LOCAL copy of the jobs we need, kept up to date from Job's events (Step 8.2). Applying then
    needs no call to the Job service: Application keeps working even if Job is down."""

    __tablename__ = "job_replica"

    job_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)  # Job's id (no new id)
    recruiter_id: Mapped[uuid.UUID]
    title: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    stages: Mapped[list] = mapped_column(JSONB)  # stage names in order
    eligibility_rules: Mapped[dict] = mapped_column(JSONB, server_default=text("'{}'"))
    application_limit: Mapped[int | None]
    deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # The job's version in the event we applied. Older events (late or replayed) are ignored
    source_version: Mapped[int]
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Application(Base, TimestampMixin):
    __tablename__ = "applications"
    __table_args__ = (
        CheckConstraint(_in("status", STATUSES), name="status"),
        CheckConstraint("score BETWEEN 0 AND 100", name="score_range"),
        CheckConstraint(_in("score_status", SCORE_STATUSES), name="score_status"),
        # The final guard against applying twice, even when two requests race: 409
        UniqueConstraint("candidate_id", "job_id", name="uq_applications_candidate_job"),
        # Recruiter board: WHERE job_id = ? [AND current_stage = ?] ORDER BY score DESC NULLS LAST
        Index("ix_applications_job_stage_score", "job_id", "current_stage", text("score DESC NULLS LAST")),
        # "My applications": WHERE candidate_id = ? ORDER BY created_at DESC
        Index("ix_applications_candidate_created", "candidate_id", text("created_at DESC")),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    # A real foreign key: job_replica is OUR table, so the database can enforce it
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("job_replica.job_id"))
    candidate_id: Mapped[uuid.UUID]  # no FK: candidates live in the Candidate service
    # Snapshot at apply time, so the recruiter board is one query (no call per row to Candidate)
    candidate_name: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, server_default=text("'SUBMITTED'"))
    current_stage: Mapped[str] = mapped_column(Text)
    score: Mapped[Decimal | None] = mapped_column(Numeric(5, 2))  # Phase 10 (Scoring)
    score_status: Mapped[str] = mapped_column(Text, server_default=text("'PENDING'"))
    version: Mapped[int] = mapped_column(server_default=text("1"))  # optimistic lock for recruiter edits


class StageHistory(Base):
    """Append-only: every move of an application, never updated or deleted."""

    __tablename__ = "application_stage_history"
    __table_args__ = (Index("ix_stage_history_app_changed", "application_id", "changed_at"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    application_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("applications.id"))
    from_stage: Mapped[str | None] = mapped_column(Text)  # NULL for the first entry (SUBMITTED)
    to_stage: Mapped[str] = mapped_column(Text)
    changed_by: Mapped[uuid.UUID]
    reason: Mapped[str | None] = mapped_column(Text)
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ApplicationNote(Base):
    __tablename__ = "application_notes"
    __table_args__ = (
        CheckConstraint("length(body) > 0", name="body_not_empty"),
        Index("ix_application_notes_app_created", "application_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    application_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("applications.id"))
    recruiter_id: Mapped[uuid.UUID]
    body: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ApplicationQuota(Base):
    """One row per candidate: how many applications they have open. Applying LOCKS this row, so two
    simultaneous applies by the same candidate take turns. Counting rows instead can't be locked:
    rows that don't exist yet can't be locked ("phantoms"), so both requests would see 9 of 10."""

    __tablename__ = "application_quotas"
    __table_args__ = (CheckConstraint("active_count >= 0", name="active_count_non_negative"),)

    candidate_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    active_count: Mapped[int] = mapped_column(server_default=text("0"))


class AuditEntry(Base):
    """Append-only: who did what to which entity, with before/after."""

    __tablename__ = "audit_entries"
    __table_args__ = (Index("ix_audit_entries_entity", "entity_type", "entity_id", "created_at"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    actor_id: Mapped[uuid.UUID]
    action: Mapped[str] = mapped_column(Text)  # STAGE_CHANGED, NOTE_ADDED, DECISION_MADE
    entity_type: Mapped[str] = mapped_column(Text)
    entity_id: Mapped[uuid.UUID]
    before: Mapped[dict | None] = mapped_column(JSONB)
    after: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
