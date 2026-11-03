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
