"""native enums (design v2.2): jobs.status -> job_status, jobs.employment_type -> employment_type

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-26
"""
import sqlalchemy as sa
from alembic import op
from smarthire_common.migration_ops import column_to_enum, column_to_text, create_enum, drop_enum

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None

STATUSES = ("DRAFT", "PROCESSING", "READY", "FAILED", "CLOSED")
EMPLOYMENT_TYPES = ("FULL_TIME", "PART_TIME", "CONTRACT", "INTERNSHIP")


def _ready_index() -> None:
    op.create_index("ix_jobs_ready_created", "jobs", [sa.text("created_at DESC")],
                    postgresql_where=sa.text("status = 'READY'"))


def upgrade() -> None:
    # The partial index's predicate was stored as text = text: it can't survive the type change
    op.drop_index("ix_jobs_ready_created", table_name="jobs")
    for name in ("ck_jobs_status", "ck_jobs_employment_type"):
        op.drop_constraint(op.f(name), "jobs", type_="check")
    create_enum("job_status", STATUSES)
    create_enum("employment_type", EMPLOYMENT_TYPES)
    column_to_enum("jobs", "status", "job_status", default="DRAFT")
    column_to_enum("jobs", "employment_type", "employment_type")
    _ready_index()


def downgrade() -> None:
    op.drop_index("ix_jobs_ready_created", table_name="jobs")
    column_to_text("jobs", "status", default="DRAFT")
    column_to_text("jobs", "employment_type")
    drop_enum("job_status")
    drop_enum("employment_type")
    op.create_check_constraint(op.f("ck_jobs_status"), "jobs", "status IN ('DRAFT', 'PROCESSING', 'READY', 'FAILED', 'CLOSED')")
    op.create_check_constraint(op.f("ck_jobs_employment_type"), "jobs",
                               "employment_type IN ('FULL_TIME', 'PART_TIME', 'CONTRACT', 'INTERNSHIP')")
    _ready_index()
