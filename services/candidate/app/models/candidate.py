"""Appendix E.6. A candidate's profile and their resumes (file metadata only: the files themselves
live in MinIO, the row keeps the object key). Rules the database can enforce are constraints."""

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Numeric, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from smarthire_common.models import Base, TimestampMixin, new_id

PDF = "application/pdf"
DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
CONTENT_TYPES = (PDF, DOCX)
RESUME_STATUSES = ("UPLOADED", "PARSED", "FAILED")
MAX_RESUME_BYTES = 5 * 1024 * 1024  # 5242880


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class Candidate(Base, TimestampMixin):
    __tablename__ = "candidates"
    __table_args__ = (CheckConstraint("experience_years >= 0", name="experience_years_non_negative"),)

    # The SAME id as the Identity user (no new id, and no foreign key: users live in Identity's DB)
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    full_name: Mapped[str] = mapped_column(Text, server_default=text("''"))
    headline: Mapped[str | None] = mapped_column(Text)
    # jsonb: read/written as whole lists, never filtered on in Postgres
    skills: Mapped[list] = mapped_column(JSONB, server_default=text("'[]'"))
    experience_years: Mapped[Decimal | None] = mapped_column(Numeric(4, 1))
    education: Mapped[list] = mapped_column(JSONB, server_default=text("'[]'"))


class Resume(Base):
    __tablename__ = "resumes"
    __table_args__ = (
        CheckConstraint(_in("content_type", CONTENT_TYPES), name="content_type"),
        CheckConstraint(f"size_bytes > 0 AND size_bytes <= {MAX_RESUME_BYTES}", name="size_bytes"),
        CheckConstraint(_in("status", RESUME_STATUSES), name="status"),
        # At most ONE active resume per candidate, even if two uploads race: a partial unique
        # index only covers rows WHERE is_active, so inactive (old) resumes don't count
        Index("uq_resumes_one_active", "candidate_id", unique=True, postgresql_where=text("is_active")),
        # "This candidate's resumes, newest first"
        Index("ix_resumes_candidate_uploaded", "candidate_id", text("uploaded_at DESC")),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    candidate_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("candidates.id", ondelete="CASCADE"))
    object_key: Mapped[str] = mapped_column(Text, unique=True)  # where the file is in the bucket
    file_name: Mapped[str] = mapped_column(Text)  # the uploader's name for it, for display only
    content_type: Mapped[str] = mapped_column(Text)
    size_bytes: Mapped[int]
    status: Mapped[str] = mapped_column(Text, server_default=text("'UPLOADED'"))
    parsed_data: Mapped[dict | None] = mapped_column(JSONB)  # Phase 10
    parse_error: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(server_default=text("true"))
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    parsed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
