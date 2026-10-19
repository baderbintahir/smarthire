"""Test infrastructure: real Postgres, Redis and MinIO in throwaway containers, schema built by the
real migrations. Every test starts with empty tables and an empty bucket."""

from pathlib import Path

import pytest
from alembic import command
from smarthire_common import testing
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

SERVICE_DIR = Path(__file__).resolve().parents[1]
TABLES = ["candidates", "resumes"]
BUCKET = "resumes"
# Same image as docker-compose.yml. Testcontainers' default (minio/minio) is no longer published.
MINIO_IMAGE = "pgsty/minio:RELEASE.2026-08-04T00-00-00Z"
# Throwaway container credentials, like the Postgres test password in smarthire_common.testing
S3_ACCESS_KEY, S3_SECRET_KEY = "test-access-key", "test-secret-key"


@pytest.fixture(scope="session")
def database_url():
    with testing.service_postgres("candidate") as url:
        yield url


@pytest.fixture(scope="session")
def migrated(database_url):
    # alembic/env.py builds Settings(), which requires the S3 keys too
    cfg = testing.alembic_config(
        SERVICE_DIR,
        database_url=database_url,
        schema="candidate",
        extra_env={"S3_ACCESS_KEY": S3_ACCESS_KEY, "S3_SECRET_KEY": S3_SECRET_KEY},
    )
    command.upgrade(cfg, "head")
    return cfg


@pytest.fixture
async def session(migrated, database_url):
    """A session connected as candidate_user, on freshly emptied tables."""
    await testing.truncate(database_url, TABLES)
    engine = create_async_engine(database_url)
    async with AsyncSession(engine, expire_on_commit=False) as s:
        yield s
    await engine.dispose()


# ---------- API tests: the real app, real Postgres + Redis + MinIO, tokens from a test key ----------


@pytest.fixture(scope="session")
def redis_url():
    with testing.redis_container() as url:
        yield url


@pytest.fixture(scope="session")
def minio():
    """A MinIO server and a client for it (tests use it to look inside the bucket)."""
    from testcontainers.community.minio import MinioContainer

    with MinioContainer(MINIO_IMAGE, access_key=S3_ACCESS_KEY, secret_key=S3_SECRET_KEY) as container:
        client = container.get_client()
        client.make_bucket(BUCKET)
        yield container.get_config()["endpoint"], client


@pytest.fixture
def bucket_objects(minio):
    """The keys currently in the bucket."""
    _, client = minio
    return lambda: sorted(obj.object_name for obj in client.list_objects(BUCKET, recursive=True))


@pytest.fixture(scope="session")
def tokens():
    return testing.TokenFactory()


@pytest.fixture
def settings(migrated, database_url, redis_url, minio):
    from app.config import Settings

    return Settings(
        database_url=database_url,
        redis_url=redis_url,
        s3_endpoint=minio[0],
        s3_access_key=S3_ACCESS_KEY,
        s3_secret_key=S3_SECRET_KEY,
        _env_file=None,
    )


@pytest.fixture
def client(settings, database_url, redis_url, minio, tokens):
    from fastapi.testclient import TestClient

    from app.main import create_app

    testing.run(testing.truncate(database_url, TABLES))
    testing.run(testing.flush_redis(redis_url))
    _, s3 = minio
    for obj in list(s3.list_objects(BUCKET, recursive=True)):
        s3.remove_object(BUCKET, obj.object_name)
    with TestClient(create_app(settings, key_provider=tokens.key_provider)) as c:
        yield c
