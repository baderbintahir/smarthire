import uuid

from sqlalchemy import CheckConstraint, ForeignKey, Index, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column
from smarthire_common.models import Base, TimestampMixin, new_id

# Appendix E.4. Statuses/roles are text + CHECK (not Postgres ENUM): adding a value later is a
# simple constraint swap instead of an ALTER TYPE.
ROLES = ("CANDIDATE", "RECRUITER", "ADMIN")
STATUSES = ("ACTIVE", "SUSPENDED")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class User(Base, TimestampMixin):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint(_in("role", ROLES), name="role"),  # -> ck_users_role
        CheckConstraint(_in("status", STATUSES), name="status"),  # -> ck_users_status
        # Case-insensitive uniqueness: Bob@x.com and bob@x.com are the same account
        Index("uq_users_email_lower", func.lower(text("email")), unique=True),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    email: Mapped[str] = mapped_column(Text)
    password_hash: Mapped[str] = mapped_column(Text)
    role: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, server_default=text("'ACTIVE'"))
    # Bumped by "logout everywhere"; copied into every access token as a claim
    token_version: Mapped[int] = mapped_column(server_default=text("1"))


class RecruiterProfile(Base, TimestampMixin):
    __tablename__ = "recruiter_profiles"

    # user_id is the primary key: the database itself guarantees one profile per user
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    display_name: Mapped[str] = mapped_column(Text)
    company: Mapped[str | None] = mapped_column(Text)
