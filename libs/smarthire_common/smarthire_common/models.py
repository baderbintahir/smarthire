import uuid
from datetime import datetime

from sqlalchemy import DateTime, MetaData, func
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
