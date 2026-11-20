"""roles as data, name and employer on users: EXPAND step (Phase 14R, R1; Appendix E.4)

Adds roles (seeded), user_roles (backfilled from users.role) and users.full_name / current_company
(backfilled from recruiter_profiles). Drops nothing: users.role and recruiter_profiles go in the
contract migration once no code reads them.

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-20
"""
import sqlalchemy as sa
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "roles",
        sa.Column("id", sa.SmallInteger(), autoincrement=False, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_roles"),
        sa.UniqueConstraint("name", name="uq_roles_name"),
    )
    # Seed data lives in the migration: every environment gets the same ids
    op.execute("INSERT INTO roles (id, name) VALUES (1, 'CANDIDATE'), (2, 'RECRUITER'), (3, 'ADMIN')")

    op.create_table(
        "user_roles",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("role_id", sa.SmallInteger(), nullable=False),
        sa.Column("granted_by", sa.Uuid(), nullable=True),
        sa.Column("granted_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("user_id", "role_id", name="pk_user_roles"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_user_roles_user_id_users", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["role_id"], ["roles.id"], name="fk_user_roles_role_id_roles"),
    )
    op.create_index("ix_user_roles_role_id", "user_roles", ["role_id"])
    # Backfill: every existing user keeps the one role they had, granted when they registered
    op.execute(
        "INSERT INTO user_roles (user_id, role_id, granted_at) "
        "SELECT u.id, r.id, u.created_at FROM users u JOIN roles r ON r.name = u.role"
    )

    # Nullable first, backfill, then NOT NULL: adding a NOT NULL column to a non-empty table needs values
    op.add_column("users", sa.Column("full_name", sa.Text(), nullable=True))
    op.add_column("users", sa.Column("current_company", sa.Text(), nullable=True))
    op.execute(
        "UPDATE users SET full_name = rp.display_name, current_company = rp.company "
        "FROM recruiter_profiles rp WHERE rp.user_id = users.id"
    )
    # Candidates' names live in the candidate schema, which Identity may not read (rule 1). Dev data
    # gets the email's local part; users correct it with PATCH /users/me.
    op.execute("UPDATE users SET full_name = split_part(email, '@', 1) WHERE coalesce(full_name, '') = ''")
    op.alter_column("users", "full_name", nullable=False)


def downgrade() -> None:
    op.drop_column("users", "current_company")
    op.drop_column("users", "full_name")
    op.drop_index("ix_user_roles_role_id", table_name="user_roles")
    op.drop_table("user_roles")
    op.drop_table("roles")
