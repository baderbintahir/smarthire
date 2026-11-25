"""native enums (design v2.2): users.status -> user_status; roles table -> user_roles.role (user_role)

Revision ID: 0008
Revises: 0007
Create Date: 2026-11-26
"""
import sqlalchemy as sa
from alembic import op
from smarthire_common.migration_ops import column_to_enum, column_to_text, create_enum, drop_enum

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None

STATUSES = ("ACTIVE", "SUSPENDED")
ROLES = ("CANDIDATE", "RECRUITER", "ADMIN")  # same order as the old role ids 1, 2, 3


def upgrade() -> None:
    create_enum("user_status", STATUSES)
    op.drop_constraint(op.f("ck_users_status"), "users", type_="check")  # the type itself rejects other values now
    column_to_enum("users", "status", "user_status", default="ACTIVE")

    # Roles: the lookup table becomes an enum column. Backfill by name, then swap the key
    create_enum("user_role", ROLES)
    op.add_column("user_roles", sa.Column("role", sa.Text(), nullable=True))
    op.execute("UPDATE user_roles ur SET role = r.name FROM roles r WHERE r.id = ur.role_id")
    column_to_enum("user_roles", "role", "user_role")
    op.alter_column("user_roles", "role", nullable=False)
    op.drop_constraint("pk_user_roles", "user_roles", type_="primary")
    op.drop_index("ix_user_roles_role_id", table_name="user_roles")
    op.drop_column("user_roles", "role_id")  # drops fk_user_roles_role_id_roles with it
    op.create_primary_key("pk_user_roles", "user_roles", ["user_id", "role"])
    op.create_index("ix_user_roles_role", "user_roles", ["role"])
    op.drop_table("roles")


def downgrade() -> None:
    op.create_table(
        "roles",
        sa.Column("id", sa.SmallInteger(), autoincrement=False, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_roles"),
        sa.UniqueConstraint("name", name="uq_roles_name"),
    )
    op.execute("INSERT INTO roles (id, name) VALUES (1, 'CANDIDATE'), (2, 'RECRUITER'), (3, 'ADMIN')")
    op.add_column("user_roles", sa.Column("role_id", sa.SmallInteger(), nullable=True))
    op.execute("UPDATE user_roles ur SET role_id = r.id FROM roles r WHERE r.name = ur.role::text")
    op.alter_column("user_roles", "role_id", nullable=False)
    op.drop_constraint("pk_user_roles", "user_roles", type_="primary")
    op.drop_index("ix_user_roles_role", table_name="user_roles")
    op.drop_column("user_roles", "role")
    op.create_primary_key("pk_user_roles", "user_roles", ["user_id", "role_id"])
    op.create_foreign_key("fk_user_roles_role_id_roles", "user_roles", "roles", ["role_id"], ["id"])
    op.create_index("ix_user_roles_role_id", "user_roles", ["role_id"])
    drop_enum("user_role")

    column_to_text("users", "status", default="ACTIVE")
    op.create_check_constraint(op.f("ck_users_status"), "users", "status IN ('ACTIVE', 'SUSPENDED')")
    drop_enum("user_status")
