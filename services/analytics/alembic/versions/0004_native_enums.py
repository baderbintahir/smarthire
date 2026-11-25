"""native enums (design v2.2): facts.kind -> fact_kind; ai_usage_daily.role / kind -> user_role / ai_usage_kind

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-26
"""
from alembic import op
from smarthire_common.migration_ops import column_to_enum, column_to_text, create_enum, drop_enum

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None

FACT_KINDS = (
    "UserRegistered", "RoleGranted", "RoleRevoked", "JobReady", "JobPublishFailed", "ApplicationSubmitted",
    "ApplicationStageChanged", "ApplicationDecisionMade", "CandidateScored", "SystemFailure",
    "AssistantQueryCompleted",
)


def upgrade() -> None:
    op.drop_constraint(op.f("ck_facts_kind"), "facts", type_="check")  # the type rejects other kinds now
    create_enum("fact_kind", FACT_KINDS)
    create_enum("user_role", ("CANDIDATE", "RECRUITER", "ADMIN"))
    create_enum("ai_usage_kind", ("QUERY", "SUMMARY"))
    column_to_enum("facts", "kind", "fact_kind")
    # Both are primary key columns: Postgres rebuilds pk_ai_usage_daily with the new types
    column_to_enum("ai_usage_daily", "role", "user_role")
    column_to_enum("ai_usage_daily", "kind", "ai_usage_kind")


def downgrade() -> None:
    column_to_text("facts", "kind")
    column_to_text("ai_usage_daily", "role")
    column_to_text("ai_usage_daily", "kind")
    for type_name in ("fact_kind", "user_role", "ai_usage_kind"):
        drop_enum(type_name)
    labels = ", ".join(f"'{kind}'" for kind in FACT_KINDS)
    op.create_check_constraint(op.f("ck_facts_kind"), "facts", f"kind IN ({labels})")
