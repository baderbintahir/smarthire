"""jobs and job_stages (Appendix E.5)

Revision ID: 0001
Revises:
Create Date: 2026-10-19
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # op.f("..."): the name is final. Without it the naming convention (ck_<table>_<name>) would be
    # applied on top of an already-full name -> "ck_jobs_ck_jobs_status".
    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("recruiter_id", sa.Uuid(), nullable=False),  # no FK: the user lives in Identity
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("location", sa.Text(), nullable=True),
        sa.Column("employment_type", sa.Text(), nullable=False),
        sa.Column("required_skills", postgresql.JSONB(), server_default=sa.text("'[]'"), nullable=False),
        sa.Column("eligibility_rules", postgresql.JSONB(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("application_limit", sa.Integer(), nullable=True),
        sa.Column("deadline", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), server_default=sa.text("'DRAFT'"), nullable=False),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_jobs"),
        sa.CheckConstraint("length(title) BETWEEN 3 AND 200", name=op.f("ck_jobs_title_length")),
        sa.CheckConstraint(
            "employment_type IN ('FULL_TIME', 'PART_TIME', 'CONTRACT', 'INTERNSHIP')", name=op.f("ck_jobs_employment_type")
        ),
        sa.CheckConstraint("application_limit > 0", name=op.f("ck_jobs_application_limit_positive")),
        sa.CheckConstraint(
            "status IN ('DRAFT', 'PROCESSING', 'READY', 'FAILED', 'CLOSED')", name=op.f("ck_jobs_status")
        ),
    )
    # "My jobs" for a recruiter: equality columns first, then the sort column
    op.create_index(
        "ix_jobs_recruiter_status_created", "jobs", ["recruiter_id", "status", sa.text("created_at DESC")]
    )
    # Public job board: only READY jobs are ever listed, so only they are indexed
    op.create_index(
        "ix_jobs_ready_created", "jobs", [sa.text("created_at DESC")], postgresql_where=sa.text("status = 'READY'")
    )

    op.create_table(
        "job_stages",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_job_stages"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], name="fk_job_stages_job_id_jobs", ondelete="CASCADE"),
        sa.CheckConstraint("position >= 0", name=op.f("ck_job_stages_position_non_negative")),
        sa.UniqueConstraint("job_id", "position", name="uq_job_stages_job_position"),
        sa.UniqueConstraint("job_id", "name", name="uq_job_stages_job_name"),
    )


def downgrade() -> None:
    op.drop_table("job_stages")  # child first: it references jobs
    op.drop_index("ix_jobs_ready_created", table_name="jobs")
    op.drop_index("ix_jobs_recruiter_status_created", table_name="jobs")
    op.drop_table("jobs")
