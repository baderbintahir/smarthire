from pathlib import Path

import pytest
from alembic import command
from smarthire_common import db, testing

from app.models.projections import PROJECTION_TABLES

SERVICE_DIR = Path(__file__).resolve().parents[1]
TABLES = PROJECTION_TABLES + ["processed_events"]


@pytest.fixture(scope="session")
def database_url():
    with testing.service_postgres("analytics") as url:
        yield url


@pytest.fixture(scope="session")
def migrated(database_url):
    cfg = testing.alembic_config(SERVICE_DIR, database_url=database_url, schema="analytics")
    command.upgrade(cfg, "head")
    return cfg


@pytest.fixture
async def database(migrated, database_url):
    await testing.truncate(database_url, TABLES)
    db.init_db(database_url)  # no app runs in these tests
    yield
    await db.close_db()


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

    testing.run(testing.truncate(database_url, TABLES))
    testing.run(testing.flush_redis(redis_url))
    settings = Settings(database_url=database_url, redis_url=redis_url, kafka_enabled=False, _env_file=None)
    with TestClient(create_app(settings, key_provider=tokens.key_provider)) as c:
        yield c
