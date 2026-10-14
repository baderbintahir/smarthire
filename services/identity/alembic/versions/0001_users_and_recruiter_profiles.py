"""users and recruiter_profiles (Appendix E.4)

Revision ID: 0001
Revises:
Create Date: 2026-10-13
"""
import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'ACTIVE'"), nullable=False),
        sa.Column("token_version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_users"),
        sa.CheckConstraint("role IN ('CANDIDATE', 'RECRUITER', 'ADMIN')", name="ck_users_role"),
        sa.CheckConstraint("status IN ('ACTIVE', 'SUSPENDED')", name="ck_users_status"),
    )
    # Expression index: uniqueness on lower(email), not email
    op.create_index("uq_users_email_lower", "users", [sa.text("lower(email)")], unique=True)

    op.create_table(
        "recruiter_profiles",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("company", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("user_id", name="pk_recruiter_profiles"),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], name="fk_recruiter_profiles_user_id_users", ondelete="CASCADE"
        ),
    )


def downgrade() -> None:
    op.drop_table("recruiter_profiles")  # child first: it references users
    op.drop_index("uq_users_email_lower", table_name="users")
    op.drop_table("users")
