"""native enums (design v2.2): applications.status / score_status, job_replica.status

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-26
"""
from alembic import op
from smarthire_common.migration_ops import column_to_enum, column_to_text, create_enum, drop_enum

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

STATUSES = ("SUBMITTED", "SCREENING", "IN_PROGRESS", "HIRED", "REJECTED", "WITHDRAWN", "CLOSED")
SCORE_STATUSES = ("PENDING", "SCORED", "PENDING_MANUAL")
JOB_STATUSES = ("DRAFT", "PROCESSING", "READY", "FAILED", "CLOSED")  # job_replica had no CHECK


def _labels(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def upgrade() -> None:
    for name in ("ck_applications_status", "ck_applications_score_status"):
        op.drop_constraint(op.f(name), "applications", type_="check")  # the types reject other values now
    create_enum("application_status", STATUSES)
    create_enum("score_status", SCORE_STATUSES)
    create_enum("job_status", JOB_STATUSES)
    column_to_enum("applications", "status", "application_status", default="SUBMITTED")
    column_to_enum("applications", "score_status", "score_status", default="PENDING")
    column_to_enum("job_replica", "status", "job_status")


def downgrade() -> None:
    column_to_text("applications", "status", default="SUBMITTED")
    column_to_text("applications", "score_status", default="PENDING")
    column_to_text("job_replica", "status")
    for type_name in ("application_status", "score_status", "job_status"):
        drop_enum(type_name)
    op.create_check_constraint(op.f("ck_applications_status"), "applications", f"status IN ({_labels(STATUSES)})")
    op.create_check_constraint(op.f("ck_applications_score_status"), "applications",
                               f"score_status IN ({_labels(SCORE_STATUSES)})")
