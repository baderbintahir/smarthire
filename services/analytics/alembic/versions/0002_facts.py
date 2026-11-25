"""facts: Analytics' own append-only fact log (design v2.0, Appendix E.9)

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-25
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

FACT_KINDS = (
    "UserRegistered", "RoleGranted", "RoleRevoked", "JobReady", "JobPublishFailed", "ApplicationSubmitted",
    "ApplicationStageChanged", "ApplicationDecisionMade", "CandidateScored", "SystemFailure",
    "AssistantQueryCompleted",
)


def upgrade() -> None:
    op.create_table(
        "facts",
        sa.Column("fact_id", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("data", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("fact_id", name="pk_facts"),
        sa.CheckConstraint("kind IN (" + ", ".join(f"'{k}'" for k in FACT_KINDS) + ")", name=op.f("ck_facts_kind")),
    )
    op.create_index("ix_facts_occurred_at", "facts", ["occurred_at"])


def downgrade() -> None:
    op.drop_index("ix_facts_occurred_at", table_name="facts")
    op.drop_table("facts")
