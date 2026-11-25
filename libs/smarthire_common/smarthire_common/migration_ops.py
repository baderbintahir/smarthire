"""Alembic helpers for the text -> native ENUM change (design v2.2). Migrations import these, so they
must never change behaviour: a migration has to do the same thing in every environment, forever.

Types are created without a schema name: each service's DB user has search_path = its own schema."""

from alembic import op


def create_enum(type_name: str, values: tuple[str, ...]) -> None:
    labels = ", ".join(f"'{value}'" for value in values)
    op.execute(f"CREATE TYPE {type_name} AS ENUM ({labels})")


def drop_enum(type_name: str) -> None:
    op.execute(f"DROP TYPE {type_name}")


def column_to_enum(table: str, column: str, type_name: str, *, default: str | None = None) -> None:
    """text -> enum. The default goes first: Postgres can't cast a text default to the new type itself."""
    if default is not None:
        op.execute(f"ALTER TABLE {table} ALTER COLUMN {column} DROP DEFAULT")
    op.execute(f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {type_name} USING {column}::{type_name}")
    if default is not None:
        op.execute(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT '{default}'")


def column_to_text(table: str, column: str, *, default: str | None = None) -> None:
    """enum -> text (the downgrade of column_to_enum)."""
    if default is not None:
        op.execute(f"ALTER TABLE {table} ALTER COLUMN {column} DROP DEFAULT")
    op.execute(f"ALTER TABLE {table} ALTER COLUMN {column} TYPE text USING {column}::text")
    if default is not None:
        op.execute(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT '{default}'")
