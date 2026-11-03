"""application tables (Appendix E.7) + outbox and processed_events (E.10)

Revision ID: 0001
Revises:
Create Date: 2026-10-30
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

STATUSES = "'SUBMITTED', 'SCREENING', 'IN_PROGRESS', 'HIRED', 'REJECTED', 'WITHDRAWN', 'CLOSED'"


def _timestamp(name: str) -> sa.Column:
    return sa.Column(name, sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False)


def upgrade() -> None:
    # op.f("..."): the name is final (see CLAUDE.md, MIGRATION NAMING RULE)
    op.create_table(
        "job_replica",
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("recruiter_id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("stages", postgresql.JSONB(), nullable=False),
        sa.Column("eligibility_rules", postgresql.JSONB(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("application_limit", sa.Integer(), nullable=True),
        sa.Column("deadline", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_version", sa.Integer(), nullable=False),
        _timestamp("updated_at"),
        sa.PrimaryKeyConstraint("job_id", name="pk_job_replica"),
    )

    op.create_table(
        "applications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("candidate_id", sa.Uuid(), nullable=False),
        sa.Column("candidate_name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'SUBMITTED'"), nullable=False),
        sa.Column("current_stage", sa.Text(), nullable=False),
        sa.Column("score", sa.Numeric(5, 2), nullable=True),
        sa.Column("score_status", sa.Text(), server_default=sa.text("'PENDING'"), nullable=False),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        sa.PrimaryKeyConstraint("id", name="pk_applications"),
        sa.ForeignKeyConstraint(["job_id"], ["job_replica.job_id"], name="fk_applications_job_id_job_replica"),
        sa.UniqueConstraint("candidate_id", "job_id", name="uq_applications_candidate_job"),
        sa.CheckConstraint(f"status IN ({STATUSES})", name=op.f("ck_applications_status")),
        sa.CheckConstraint("score BETWEEN 0 AND 100", name=op.f("ck_applications_score_range")),
        sa.CheckConstraint(
            "score_status IN ('PENDING', 'SCORED', 'PENDING_MANUAL')", name=op.f("ck_applications_score_status")
        ),
    )
    # Recruiter board: filter by job (+ stage), sort by score with unscored applications last
    op.create_index(
        "ix_applications_job_stage_score", "applications", ["job_id", "current_stage", sa.text("score DESC NULLS LAST")]
    )
    # A candidate's own applications, newest first
    op.create_index("ix_applications_candidate_created", "applications", ["candidate_id", sa.text("created_at DESC")])

    op.create_table(
        "application_stage_history",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("application_id", sa.Uuid(), nullable=False),
        sa.Column("from_stage", sa.Text(), nullable=True),
        sa.Column("to_stage", sa.Text(), nullable=False),
        sa.Column("changed_by", sa.Uuid(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        _timestamp("changed_at"),
        sa.PrimaryKeyConstraint("id", name="pk_application_stage_history"),
        sa.ForeignKeyConstraint(
            ["application_id"], ["applications.id"], name="fk_application_stage_history_application_id_applications"
        ),
    )
    op.create_index("ix_stage_history_app_changed", "application_stage_history", ["application_id", "changed_at"])

    op.create_table(
        "application_notes",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("application_id", sa.Uuid(), nullable=False),
        sa.Column("recruiter_id", sa.Uuid(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        _timestamp("created_at"),
        sa.PrimaryKeyConstraint("id", name="pk_application_notes"),
        sa.ForeignKeyConstraint(
            ["application_id"], ["applications.id"], name="fk_application_notes_application_id_applications"
        ),
        sa.CheckConstraint("length(body) > 0", name=op.f("ck_application_notes_body_not_empty")),
    )
    op.create_index("ix_application_notes_app_created", "application_notes", ["application_id", "created_at"])

    op.create_table(
        "application_quotas",
        sa.Column("candidate_id", sa.Uuid(), nullable=False),
        sa.Column("active_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.PrimaryKeyConstraint("candidate_id", name="pk_application_quotas"),
        sa.CheckConstraint("active_count >= 0", name=op.f("ck_application_quotas_active_count_non_negative")),
    )

    op.create_table(
        "audit_entries",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("actor_id", sa.Uuid(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_id", sa.Uuid(), nullable=False),
        sa.Column("before", postgresql.JSONB(), nullable=True),
        sa.Column("after", postgresql.JSONB(), nullable=True),
        _timestamp("created_at"),
        sa.PrimaryKeyConstraint("id", name="pk_audit_entries"),
    )
    op.create_index("ix_audit_entries_entity", "audit_entries", ["entity_type", "entity_id", "created_at"])

    # ---- E.10: events we publish (outbox) and events we consumed (processed_events) ----
    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("message_key", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("headers", postgresql.JSONB(), server_default=sa.text("'{}'"), nullable=False),
        _timestamp("created_at"),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_outbox"),
        sa.UniqueConstraint("event_id", name="uq_outbox_event_id"),
    )
    op.create_index("ix_outbox_unpublished", "outbox", ["id"], postgresql_where=sa.text("published_at IS NULL"))
    op.create_table(
        "processed_events",
        sa.Column("consumer_name", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        _timestamp("processed_at"),
        sa.PrimaryKeyConstraint("consumer_name", "event_id", name="pk_processed_events"),
    )


def downgrade() -> None:
    op.drop_table("processed_events")
    op.drop_index("ix_outbox_unpublished", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("ix_audit_entries_entity", table_name="audit_entries")
    op.drop_table("audit_entries")
    op.drop_table("application_quotas")
    op.drop_index("ix_application_notes_app_created", table_name="application_notes")
    op.drop_table("application_notes")
    op.drop_index("ix_stage_history_app_changed", table_name="application_stage_history")
    op.drop_table("application_stage_history")
    op.drop_index("ix_applications_candidate_created", table_name="applications")
    op.drop_index("ix_applications_job_stage_score", table_name="applications")
    op.drop_table("applications")  # children first: they reference job_replica
    op.drop_table("job_replica")
