"""native enum (design v2.2): notification_log.status -> email_status

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


def upgrade() -> None:
    op.drop_constraint(op.f("ck_notification_log_status"), "notification_log", type_="check")
    create_enum("email_status", ("QUEUED", "SENT", "FAILED"))
    # ix_notification_log_status_created is a plain index: Postgres rebuilds it with the new type
    column_to_enum("notification_log", "status", "email_status", default="QUEUED")


def downgrade() -> None:
    column_to_text("notification_log", "status", default="QUEUED")
    drop_enum("email_status")
    op.create_check_constraint(op.f("ck_notification_log_status"), "notification_log",
                               "status IN ('QUEUED', 'SENT', 'FAILED')")
