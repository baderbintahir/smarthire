"""CONTRACT step (Phase 14R, R8; Appendix E.4, design v1.2): drop what nothing reads any more

- users.role + ck_users_role: replaced by roles/user_roles (0005)
- recruiter_profiles: display_name/company moved to users.full_name/current_company (0005)
- refresh_tokens: refresh sessions live in Redis (identity:session:{sid}, Appendix E.13)

The downgrade rebuilds all three, backfilled from the new tables where the data still exists.
Refresh tokens can't come back (they are in Redis now): the table returns empty.

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-20
"""
import sqlalchemy as sa
from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_refresh_tokens_family_id", table_name="refresh_tokens")
    op.drop_index("ix_refresh_tokens_user_id", table_name="refresh_tokens")
    op.drop_table("refresh_tokens")
    op.drop_table("recruiter_profiles")
    op.drop_constraint(op.f("ck_users_role"), "users", type_="check")  # op.f: final name, no convention prefix
    op.drop_column("users", "role")


def downgrade() -> None:
    # users.role: one role per user again. With several, keep the lowest role id (CANDIDATE first).
    op.add_column("users", sa.Column("role", sa.Text(), nullable=True))
    op.execute(
        "UPDATE users SET role = r.name FROM ("
        "  SELECT DISTINCT ON (ur.user_id) ur.user_id, ro.name FROM user_roles ur"
        "  JOIN roles ro ON ro.id = ur.role_id ORDER BY ur.user_id, ur.role_id"
        ") r WHERE r.user_id = users.id"
    )
    op.execute("UPDATE users SET role = 'CANDIDATE' WHERE role IS NULL")  # can't happen (LAST_ROLE guard); be safe
    op.alter_column("users", "role", nullable=False)
    # op.f: the name is final; without it the naming convention would prefix it again (ck_users_ck_users_role)
    op.create_check_constraint(op.f("ck_users_role"), "users", "role IN ('CANDIDATE', 'RECRUITER', 'ADMIN')")

    op.create_table(
        "recruiter_profiles",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("company", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("user_id", name="pk_recruiter_profiles"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_recruiter_profiles_user_id_users", ondelete="CASCADE"),
    )
    op.execute(
        "INSERT INTO recruiter_profiles (user_id, display_name, company) "
        "SELECT u.id, u.full_name, u.current_company FROM users u "
        "JOIN user_roles ur ON ur.user_id = u.id JOIN roles r ON r.id = ur.role_id WHERE r.name = 'RECRUITER'"
    )

    op.create_table(
        "refresh_tokens",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("family_id", sa.Uuid(), nullable=False),
        sa.Column("token_hash", sa.LargeBinary(), nullable=False),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_refresh_tokens"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_refresh_tokens_user_id_users", ondelete="CASCADE"),
        sa.UniqueConstraint("token_hash", name="uq_refresh_tokens_token_hash"),
    )
    op.create_index("ix_refresh_tokens_user_id", "refresh_tokens", ["user_id"])
    op.create_index("ix_refresh_tokens_family_id", "refresh_tokens", ["family_id"])
