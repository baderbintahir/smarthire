import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import DateTime, MetaData, func
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from uuid6 import uuid7

# Appendix E.2: explicit, predictable constraint names (Alembic can then alter/drop them
# reliably, and names are identical in every environment).
NAMING_CONVENTION = {
    "pk": "pk_%(table_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",  # CheckConstraint(..., name="status") required
    "ix": "ix_%(column_0_N_label)s",  # label already starts with the table name
}


class Base(DeclarativeBase):
    """Base for every service's models. The schema is not set here: each service's DB user
    has its own search_path (see infra/postgres/init.sh)."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class TimestampMixin:
    """created_at / updated_at as timestamptz (UTC). updated_at is set by the repository
    layer on every update (Appendix E.2), not by an ORM hook."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


def new_id() -> uuid.UUID:
    """UUIDv7 for public primary keys: time-ordered, so new rows append to the index."""
    return uuid7()


def pg_enum(enum_class: type[Enum], name: str) -> SqlEnum:
    """A column type backed by a native PostgreSQL ENUM (design v2.2): the database itself refuses any value
    outside the list, without a CHECK constraint. The Python enum is the single source of the values; the
    column stores the enum's VALUES (e.g. "application/pdf"), not its member names.
    Migrations create the type (CREATE TYPE ... AS ENUM); adding a value later = ALTER TYPE ... ADD VALUE."""
    return SqlEnum(enum_class, name=name, values_callable=lambda members: [m.value for m in members],
                   validate_strings=True)
