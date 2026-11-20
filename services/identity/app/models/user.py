import uuid

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, SmallInteger, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column
from smarthire_common.models import Base, TimestampMixin, new_id

# Appendix E.4 (design v1.2). Statuses are text + CHECK (not Postgres ENUM): adding a value later is
# a simple constraint swap instead of an ALTER TYPE. Roles are rows (roles, user_roles), not a column.
STATUSES = ("ACTIVE", "SUSPENDED")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class User(Base, TimestampMixin):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint(_in("status", STATUSES), name="status"),  # -> ck_users_status
        # Case-insensitive uniqueness: Bob@x.com and bob@x.com are the same account
        Index("uq_users_email_lower", func.lower(text("email")), unique=True),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    email: Mapped[str] = mapped_column(Text)
    password_hash: Mapped[str] = mapped_column(Text)
    # Identity is the one source of a person's name (design v1.1): Application snapshots it from the token
    full_name: Mapped[str] = mapped_column(Text)
    current_company: Mapped[str | None] = mapped_column(Text)  # current employer, for any role
    status: Mapped[str] = mapped_column(Text, server_default=text("'ACTIVE'"))
    # Bumped by "logout everywhere"; copied into every access token as a claim
    token_version: Mapped[int] = mapped_column(server_default=text("1"))


class Role(Base):
    """Roles as data (E.4): seeded by migration 0005 (1 CANDIDATE, 2 RECRUITER, 3 ADMIN). A new role
    is a new row, not a schema change."""

    __tablename__ = "roles"

    # Fixed ids from the seed, never generated: code and migrations refer to them
    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(Text, unique=True)  # -> uq_roles_name


class UserRole(Base):
    """Which roles a user holds. The (user_id, role_id) primary key means a role is held at most once."""

    __tablename__ = "user_roles"

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    # index -> ix_user_roles_role_id ("who holds role X?"); the primary key already serves user_id lookups
    role_id: Mapped[int] = mapped_column(SmallInteger, ForeignKey("roles.id"), primary_key=True, index=True)
    granted_by: Mapped[uuid.UUID | None] = mapped_column()  # null when self-granted
    granted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
