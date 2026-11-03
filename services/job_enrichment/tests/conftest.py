"""Real MongoDB in a throwaway container (same image as docker-compose.yml). Every test starts with
an empty database."""

import pytest
from smarthire_common import testing

MONGO_IMAGE = "mongo:8.0.34"


@pytest.fixture(scope="session")
def mongo_url():
    from testcontainers.community.mongodb import MongoDbContainer

    with MongoDbContainer(MONGO_IMAGE) as mongo:
        yield mongo.get_connection_url()


@pytest.fixture
def settings(mongo_url):
    from app.config import Settings

    return Settings(mongo_url=mongo_url, mongo_database="enrichment_test", temporal_enabled=False, _env_file=None)


async def _drop(mongo_url):
    from pymongo import AsyncMongoClient

    client = AsyncMongoClient(mongo_url)
    await client.drop_database("enrichment_test")
    await client.close()


@pytest.fixture
async def mongo(settings):
    """The chassis Mongo connection for tests that call repositories/activities directly."""
    from smarthire_common.mongo import close_mongo, init_mongo

    from app.repositories import structured_jobs

    await _drop(settings.mongo_url)
    init_mongo(settings.mongo_url, settings.mongo_database)
    await structured_jobs.ensure_indexes()
    yield
    await close_mongo()


@pytest.fixture
def client(settings):
    from fastapi.testclient import TestClient

    from app.main import create_app

    testing.run(_drop(settings.mongo_url))
    with TestClient(create_app(settings)) as c:
        yield c
