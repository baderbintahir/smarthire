"""Design v2.0 (Appendix E.9): Analytics' own append-only log of facts. Workflows record facts through
POST /internal/facts; until Kafka is removed (Step T10) the consumer records events here too. The
projections are computed from it, and a rebuild replays it, so history no longer depends on anyone
else's retention."""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Index, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from smarthire_common.models import Base

FACT_KINDS = (
    "UserRegistered", "RoleGranted", "RoleRevoked", "JobReady", "JobPublishFailed", "ApplicationSubmitted",
    "ApplicationStageChanged", "ApplicationDecisionMade", "CandidateScored", "SystemFailure",
    "AssistantQueryCompleted",
)


class Fact(Base):
    __tablename__ = "facts"
    __table_args__ = (
        CheckConstraint("kind IN (" + ", ".join(f"'{k}'" for k in FACT_KINDS) + ")", name="kind"),
        Index("ix_facts_occurred_at", "occurred_at"),  # a rebuild replays in time order
    )

    # Workflow id + kind (or the Kafka event id): the same fact recorded twice is stored once
    fact_id: Mapped[str] = mapped_column(Text, primary_key=True)
    kind: Mapped[str] = mapped_column(Text)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # when it happened, not when stored
    data: Mapped[dict] = mapped_column(JSONB)  # only the fields the metrics need: ids, a title, a stage name
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
