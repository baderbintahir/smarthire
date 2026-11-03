"""Appendix E.9: the read model. Every table here is a PROJECTION: computed only from events, never
written by anyone else, so it can be dropped and rebuilt by replaying the topics (app/cli.py rebuild).

Plus one table the appendix doesn't list, `published_jobs`, to count "jobs published, first version
only" exactly: a job's row in job_application_counts may be created by an application event that
arrives before its JobReady (different topics have no order between them), so that row can't tell
whether the job was already counted."""

import uuid
from datetime import date, datetime

from sqlalchemy import BigInteger, CheckConstraint, Date, DateTime, Index, Text, text
from sqlalchemy.orm import Mapped, mapped_column
from smarthire_common.models import Base


class MetricCounter(Base):
    __tablename__ = "metric_counters"
    __table_args__ = (CheckConstraint("value >= 0", name="value_non_negative"),)

    name: Mapped[str] = mapped_column(Text, primary_key=True)  # total_candidates, total_recruiters, ...
    value: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))


class ApplicationsDaily(Base):
    __tablename__ = "applications_daily"

    day: Mapped[date] = mapped_column(Date, primary_key=True)
    count: Mapped[int] = mapped_column(server_default=text("0"))


class JobApplicationCount(Base):
    __tablename__ = "job_application_counts"
    # "Most applied jobs": ORDER BY count DESC LIMIT n
    __table_args__ = (Index("ix_job_application_counts_count", text("count DESC")),)

    job_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(Text)
    count: Mapped[int] = mapped_column(server_default=text("0"))


class CandidateApplicationCount(Base):
    __tablename__ = "candidate_application_counts"

    candidate_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    count: Mapped[int] = mapped_column(server_default=text("0"))


class ApplicationTimeline(Base):
    __tablename__ = "application_timeline"

    application_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    job_id: Mapped[uuid.UUID]
    candidate_id: Mapped[uuid.UUID]
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    interview_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    hired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class FailuresDaily(Base):
    __tablename__ = "failures_daily"

    day: Mapped[date] = mapped_column(Date, primary_key=True)
    source: Mapped[str] = mapped_column(Text, primary_key=True)
    count: Mapped[int] = mapped_column(server_default=text("0"))


class AiUsageDaily(Base):
    __tablename__ = "ai_usage_daily"

    day: Mapped[date] = mapped_column(Date, primary_key=True)
    role: Mapped[str] = mapped_column(Text, primary_key=True)  # RECRUITER, CANDIDATE
    kind: Mapped[str] = mapped_column(Text, primary_key=True)  # QUERY, SUMMARY
    count: Mapped[int] = mapped_column(server_default=text("0"))


class PublishedJob(Base):
    """Not in E.9 (see the module docstring): which jobs have been counted as published."""

    __tablename__ = "published_jobs"

    job_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    first_published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


PROJECTION_TABLES = ["metric_counters", "applications_daily", "job_application_counts", "candidate_application_counts",
                     "application_timeline", "failures_daily", "ai_usage_daily", "published_jobs"]
