"""analytics projections (Appendix E.9) + published_jobs + processed_events (E.10)

Revision ID: 0001
Revises:
Create Date: 2026-11-03
"""
import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def _count() -> sa.Column:
    return sa.Column("count", sa.Integer(), server_default=sa.text("0"), nullable=False)


def upgrade() -> None:
    op.create_table(
        "metric_counters",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("value", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.PrimaryKeyConstraint("name", name="pk_metric_counters"),
        sa.CheckConstraint("value >= 0", name=op.f("ck_metric_counters_value_non_negative")),
    )
    op.create_table("applications_daily", sa.Column("day", sa.Date(), nullable=False), _count(),
                    sa.PrimaryKeyConstraint("day", name="pk_applications_daily"))
    op.create_table("job_application_counts", sa.Column("job_id", sa.Uuid(), nullable=False),
                    sa.Column("title", sa.Text(), nullable=False), _count(),
                    sa.PrimaryKeyConstraint("job_id", name="pk_job_application_counts"))
    op.create_index("ix_job_application_counts_count", "job_application_counts", [sa.text("count DESC")])
    op.create_table("candidate_application_counts", sa.Column("candidate_id", sa.Uuid(), nullable=False), _count(),
                    sa.PrimaryKeyConstraint("candidate_id", name="pk_candidate_application_counts"))
    op.create_table(
        "application_timeline",
        sa.Column("application_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("candidate_id", sa.Uuid(), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("interview_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("hired_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("application_id", name="pk_application_timeline"),
    )
    op.create_table("failures_daily", sa.Column("day", sa.Date(), nullable=False),
                    sa.Column("source", sa.Text(), nullable=False), _count(),
                    sa.PrimaryKeyConstraint("day", "source", name="pk_failures_daily"))
    op.create_table("ai_usage_daily", sa.Column("day", sa.Date(), nullable=False), sa.Column("role", sa.Text(), nullable=False),
                    sa.Column("kind", sa.Text(), nullable=False), _count(),
                    sa.PrimaryKeyConstraint("day", "role", "kind", name="pk_ai_usage_daily"))
    op.create_table("published_jobs", sa.Column("job_id", sa.Uuid(), nullable=False),
                    sa.Column("first_published_at", sa.DateTime(timezone=True), nullable=False),
                    sa.PrimaryKeyConstraint("job_id", name="pk_published_jobs"))
    op.create_table(
        "processed_events",
        sa.Column("consumer_name", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("consumer_name", "event_id", name="pk_processed_events"),
    )


def downgrade() -> None:
    for table in ("processed_events", "published_jobs", "ai_usage_daily", "failures_daily", "application_timeline",
                  "candidate_application_counts"):
        op.drop_table(table)
    op.drop_index("ix_job_application_counts_count", table_name="job_application_counts")
    for table in ("job_application_counts", "applications_daily", "metric_counters"):
        op.drop_table(table)
