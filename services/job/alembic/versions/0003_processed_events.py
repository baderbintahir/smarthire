"""processed_events (Appendix E.10): Job now consumes its own jobs.commands topic (Step 9.5)

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-03
"""
import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Events our consumers already handled: the primary key IS the duplicate check
    op.create_table(
        "processed_events",
        sa.Column("consumer_name", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("consumer_name", "event_id", name="pk_processed_events"),
    )


def downgrade() -> None:
    op.drop_table("processed_events")
