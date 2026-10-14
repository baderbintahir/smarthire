import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine

import app.models  # noqa: F401  (importing registers the models on Base.metadata)
from app.config import Settings
from smarthire_common.models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

settings = Settings()
target_metadata = Base.metadata  # what `alembic revision --autogenerate` compares the DB to


def _include_object(obj, name, type_, reflected, compare_to) -> bool:
    # Models don't name a schema (search_path does), but the version table is named with an
    # explicit schema, so autogenerate wouldn't recognise it as Alembic's own: skip it
    return not (type_ == "table" and name == "alembic_version")


def _configure(connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_object=_include_object,
        # Keep Alembic's own bookkeeping table in the service's schema: the service's DB user
        # has no rights on "public", and each service has its own migration history.
        version_table_schema=settings.db_schema,
    )


def run_migrations_offline() -> None:
    context.configure(
        url=settings.database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        version_table_schema=settings.db_schema,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = create_async_engine(settings.database_url, poolclass=pool.NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(_do_run)
    await engine.dispose()


def _do_run(connection) -> None:
    _configure(connection)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
