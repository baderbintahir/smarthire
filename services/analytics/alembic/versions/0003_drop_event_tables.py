"""drop processed_events (design v2.0, contract step): follow-ups are orchestrator workflow steps now

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-25
"""
import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Nothing writes or reads these since Step T9 (no outbox relay, no Kafka consumer); Kafka is gone (T10)
    op.drop_table("processed_events")


def downgrade() -> None:
    # Recreated empty, exactly as before (Appendix E.10 of design v1.2)
    op.create_table(
        "processed_events",
        sa.Column("consumer_name", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("consumer_name", "event_id", name="pk_processed_events"),
    )
