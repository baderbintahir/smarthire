"""fix doubled CHECK constraint names

0001 passed full names ("ck_users_role") and the naming convention (ck_<table>_<name>) was applied on
top, giving "ck_users_ck_users_role". Renamed here instead of editing 0001, which has already run
(Appendix E.3). Renaming a constraint is metadata-only: no table rewrite, no lock held for long.

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-19
"""
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

RENAMES = [
    ("users", "ck_users_ck_users_role", "ck_users_role"),
    ("users", "ck_users_ck_users_status", "ck_users_status"),
]


def upgrade() -> None:
    for table, old, new in RENAMES:
        op.execute(f"ALTER TABLE {table} RENAME CONSTRAINT {old} TO {new}")


def downgrade() -> None:
    for table, old, new in RENAMES:
        op.execute(f"ALTER TABLE {table} RENAME CONSTRAINT {new} TO {old}")
