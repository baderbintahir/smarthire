import uuid

from datetime import datetime

from enum import StrEnum

from sqlalchemy import DateTime, ForeignKey, Index, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column
from smarthire_common.models import Base, TimestampMixin, new_id, pg_enum

# Appendix E.4 (design v2.2): fixed value sets are native PostgreSQL ENUMs. StrEnum members compare
# equal to their strings, so `user.status == "ACTIVE"` keeps working.


class UserStatus(StrEnum):
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"


class Role(StrEnum):
    """Declaration order = sort order (Postgres sorts enums the same way): tokens list roles in it."""

    CANDIDATE = "CANDIDATE"
    RECRUITER = "RECRUITER"
    ADMIN = "ADMIN"


class User(Base, TimestampMixin):
    __tablename__ = "users"
    __table_args__ = (
        # Case-insensitive uniqueness: Bob@x.com and bob@x.com are the same account
        Index("uq_users_email_lower", func.lower(text("email")), unique=True),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    email: Mapped[str] = mapped_column(Text)
    password_hash: Mapped[str] = mapped_column(Text)
    # Identity is the one source of a person's name (design v1.1): Application snapshots it from the token
    full_name: Mapped[str] = mapped_column(Text)
    current_company: Mapped[str | None] = mapped_column(Text)  # current employer, for any role
    status: Mapped[UserStatus] = mapped_column(pg_enum(UserStatus, "user_status"), server_default=text("'ACTIVE'"))
    # Bumped by "logout everywhere"; copied into every access token as a claim
    token_version: Mapped[int] = mapped_column(server_default=text("1"))


class UserRole(Base):
    """Which roles a user holds. The (user_id, role) primary key means a role is held at most once."""

    __tablename__ = "user_roles"

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    # index -> ix_user_roles_role ("who holds role X?"); the primary key already serves user_id lookups
    role: Mapped[Role] = mapped_column(pg_enum(Role, "user_role"), primary_key=True, index=True)
    granted_by: Mapped[uuid.UUID | None] = mapped_column()  # null when self-granted
    granted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
