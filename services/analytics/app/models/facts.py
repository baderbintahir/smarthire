"""Design v2.0 (Appendix E.9): Analytics' own append-only log of facts. Workflows record facts through
POST /internal/facts (the facts from before design v2.0 were copied in from Kafka once, in Step T4). The
projections are computed from it, and a rebuild replays it, so history depends on no one else's retention."""

from datetime import datetime
from enum import StrEnum

from sqlalchemy import DateTime, Index, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from smarthire_common.models import Base, pg_enum



class FactKind(StrEnum):
    """A native PostgreSQL ENUM, fact_kind (design v2.2). A new kind = ALTER TYPE fact_kind ADD VALUE."""

    USER_REGISTERED = "UserRegistered"
    ROLE_GRANTED = "RoleGranted"
    ROLE_REVOKED = "RoleRevoked"
    JOB_READY = "JobReady"
    JOB_PUBLISH_FAILED = "JobPublishFailed"
    APPLICATION_SUBMITTED = "ApplicationSubmitted"
    APPLICATION_STAGE_CHANGED = "ApplicationStageChanged"
    APPLICATION_DECISION_MADE = "ApplicationDecisionMade"
    CANDIDATE_SCORED = "CandidateScored"
    SYSTEM_FAILURE = "SystemFailure"
    ASSISTANT_QUERY_COMPLETED = "AssistantQueryCompleted"


class Fact(Base):
    __tablename__ = "facts"
    __table_args__ = (
        Index("ix_facts_occurred_at", "occurred_at"),  # a rebuild replays in time order
    )

    # Workflow run + kind (or, for facts from before v2.0, the Kafka event id): stored once
    fact_id: Mapped[str] = mapped_column(Text, primary_key=True)
    kind: Mapped[FactKind] = mapped_column(pg_enum(FactKind, "fact_kind"))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # when it happened, not when stored
    data: Mapped[dict] = mapped_column(JSONB)  # only the fields the metrics need: ids, a title, a stage name
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
