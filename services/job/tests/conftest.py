"""Test infrastructure: real Postgres in a throwaway container (smarthire_common.testing), schema
built by the real migrations. Every test starts with empty tables."""

from pathlib import Path

import pytest
from alembic import command
from smarthire_common import testing
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

SERVICE_DIR = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def database_url():
    with testing.service_postgres("job") as url:
        yield url


@pytest.fixture(scope="session")
def migrated(database_url):
    cfg = testing.alembic_config(SERVICE_DIR, database_url=database_url, schema="job")
    command.upgrade(cfg, "head")
    return cfg


@pytest.fixture
async def session(migrated, database_url):
    """A session connected as job_user, on freshly emptied tables."""
    await testing.truncate(database_url, ["jobs", "job_stages"])
    engine = create_async_engine(database_url)
    async with AsyncSession(engine, expire_on_commit=False) as s:
        yield s
    await engine.dispose()


# ---------- API tests: the real app, real Postgres + Redis, tokens from a test key ----------


@pytest.fixture(scope="session")
def redis_url():
    with testing.redis_container() as url:
        yield url


@pytest.fixture(scope="session")
def tokens():
    return testing.TokenFactory()


@pytest.fixture
def client(migrated, database_url, redis_url, tokens):
    from fastapi.testclient import TestClient

    from app.config import Settings
    from app.main import create_app

    testing.run(testing.truncate(database_url, ["jobs", "job_stages"]))
    testing.run(testing.flush_redis(redis_url))
    settings = Settings(database_url=database_url, redis_url=redis_url, _env_file=None)
    with TestClient(create_app(settings, key_provider=tokens.key_provider)) as c:
        yield c
