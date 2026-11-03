"""notification tables (Appendix E.8) + processed_events (E.10) + the email templates

Revision ID: 0001
Revises:
Create Date: 2026-11-03
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

# Plain-text templates; {placeholders} come from the log row's payload (app/domain/templates.py)
TEMPLATES = [
    ("APPLICATION_RECEIVED", "We received your application for {job_title}",
     "Hello,\n\nThank you for applying for {job_title}. We will let you know as soon as there is news.\n\n- SmartHire"),
    ("STAGE_CHANGED", "Your application for {job_title}: {to_stage}",
     "Hello,\n\nYour application for {job_title} has moved to the {to_stage} stage.\n\n- SmartHire"),
    ("DECISION_MADE", "An update on your application for {job_title}",
     "Hello,\n\n{decision_message}\n\n- SmartHire"),
    ("JOB_READY", "Your job \"{job_title}\" is live",
     "Hello,\n\nYour job \"{job_title}\" has been published and is accepting applications.\n\n- SmartHire"),
    ("JOB_PUBLISH_FAILED", "Your job could not be published",
     "Hello,\n\nWe could not publish your job: {reason}\nPlease try publishing it again.\n\n- SmartHire"),
]


def upgrade() -> None:
    templates = op.create_table(
        "notification_templates",
        sa.Column("code", sa.Text(), nullable=False),
        sa.Column("subject", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("code", name="pk_notification_templates"),
    )
    op.bulk_insert(templates, [{"code": c, "subject": s, "body": b} for c, s, b in TEMPLATES])

    op.create_table(
        "notification_log",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("notification_key", sa.Text(), nullable=False),
        sa.Column("recipient", sa.Text(), nullable=False),
        sa.Column("template_code", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'QUEUED'"), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_notification_log"),
        sa.UniqueConstraint("notification_key", name="uq_notification_log_notification_key"),
        sa.ForeignKeyConstraint(["template_code"], ["notification_templates.code"],
                                name="fk_notification_log_template_code_notification_templates"),
        sa.CheckConstraint("status IN ('QUEUED', 'SENT', 'FAILED')", name=op.f("ck_notification_log_status")),
        sa.CheckConstraint("attempts >= 0", name=op.f("ck_notification_log_attempts_non_negative")),
    )
    op.create_index("ix_notification_log_status_created", "notification_log", ["status", "created_at"])

    op.create_table(
        "processed_events",
        sa.Column("consumer_name", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("consumer_name", "event_id", name="pk_processed_events"),
    )


def downgrade() -> None:
    op.drop_table("processed_events")
    op.drop_index("ix_notification_log_status_created", table_name="notification_log")
    op.drop_table("notification_log")
    op.drop_table("notification_templates")
