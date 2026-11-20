"""drop candidates.full_name (Phase 14R, R5; Appendix E.6, design v1.1)

A person's name lives in Identity (users.full_name). Nothing reads this column any more:
Application takes the name from the token's `name` claim.

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-20
"""
import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("candidates", "full_name")


def downgrade() -> None:
    # The names themselves can't come back (they were dropped); the column returns empty, as in 0001
    op.add_column("candidates", sa.Column("full_name", sa.Text(), server_default=sa.text("''"), nullable=False))
