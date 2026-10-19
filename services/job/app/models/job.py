"""Appendix E.5. A Job and its stages form one aggregate: stages only change through their job,
in the same transaction. Rules the database can enforce are constraints, not just code."""

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from smarthire_common.models import Base, TimestampMixin, new_id

EMPLOYMENT_TYPES = ("FULL_TIME", "PART_TIME", "CONTRACT", "INTERNSHIP")
STATUSES = ("DRAFT", "PROCESSING", "READY", "FAILED", "CLOSED")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class Job(Base, TimestampMixin):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint("length(title) BETWEEN 3 AND 200", name="title_length"),
        CheckConstraint(_in("employment_type", EMPLOYMENT_TYPES), name="employment_type"),
        CheckConstraint("application_limit > 0", name="application_limit_positive"),
        CheckConstraint(_in("status", STATUSES), name="status"),
        # "My jobs": WHERE recruiter_id = ? AND status = ? ORDER BY created_at DESC
        # (equality columns first, then the sort column)
        Index("ix_jobs_recruiter_status_created", "recruiter_id", "status", text("created_at DESC")),
        # Public job board: WHERE status = 'READY' ORDER BY created_at DESC.
        # Partial index: holds READY rows only, so drafts/closed jobs don't make it bigger
        Index("ix_jobs_ready_created", text("created_at DESC"), postgresql_where=text("status = 'READY'")),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    # No foreign key: the user lives in Identity's database (database per service)
    recruiter_id: Mapped[uuid.UUID]
    title: Mapped[str] = mapped_column(Text)
    description: Mapped[str] = mapped_column(Text)
    location: Mapped[str | None] = mapped_column(Text)
    employment_type: Mapped[str] = mapped_column(Text)
    # jsonb: always read/written as one list, never filtered on in Postgres (search happens in
    # Job Enrichment / Semantic Index)
    required_skills: Mapped[list] = mapped_column(JSONB, server_default=text("'[]'"))
    eligibility_rules: Mapped[dict] = mapped_column(JSONB, server_default=text("'{}'"))
    application_limit: Mapped[int | None]
    deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(Text, server_default=text("'DRAFT'"))
    # Optimistic lock for edits (Step 5.3) and the content version the publishing workflow
    # processes (Phase 9)
    version: Mapped[int] = mapped_column(server_default=text("1"))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class JobStage(Base):
    """A table, not jsonb: order and uniqueness are rules the database must enforce."""

    __tablename__ = "job_stages"
    __table_args__ = (
        CheckConstraint("position >= 0", name="position_non_negative"),
        UniqueConstraint("job_id", "position", name="uq_job_stages_job_position"),
        UniqueConstraint("job_id", "name", name="uq_job_stages_job_name"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(Text)
    position: Mapped[int]
