"""Real Postgres (own user + schema, schema built by the real migrations). Every test starts empty
(the seeded templates are kept)."""

import os
from pathlib import Path

import pytest
from alembic import command
from smarthire_common import db, testing

# Importing the Celery app (app.tasks) builds Settings() at import time, as the worker does
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://unused@localhost/unused")
os.environ.setdefault("CELERY_BROKER_URL", "memory://")

SERVICE_DIR = Path(__file__).resolve().parents[1]
TABLES = ["notification_log", "processed_events"]  # NOT notification_templates: seeded by the migration


@pytest.fixture(scope="session")
def database_url():
    with testing.service_postgres("notification") as url:
        yield url


@pytest.fixture(scope="session")
def migrated(database_url):
    cfg = testing.alembic_config(SERVICE_DIR, database_url=database_url, schema="notification")
    command.upgrade(cfg, "head")
    return cfg


@pytest.fixture
async def database(migrated, database_url):
    await testing.truncate(database_url, TABLES)
    db.init_db(database_url)  # no app runs in these tests
    yield
    await db.close_db()


@pytest.fixture(scope="session")
def service_tokens():
    return testing.ServiceTokenFactory()  # the orchestrator's tokens for /internal/*


@pytest.fixture
def make_client(migrated, database_url, service_tokens):
    """A TestClient with a fake mailer: make_client(mailer) -> client."""
    from contextlib import ExitStack

    from fastapi.testclient import TestClient

    from app.config import Settings
    from app.main import create_app

    testing.run(testing.truncate(database_url, TABLES))
    stack = ExitStack()

    def make(mailer):
        settings = Settings(database_url=database_url, kafka_enabled=False, _env_file=None)
        return stack.enter_context(TestClient(create_app(settings, service_key_provider=service_tokens.key_provider,
                                                         mailer=mailer)))

    yield make
    stack.close()
