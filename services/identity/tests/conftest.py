"""Test infrastructure: real Postgres + Redis in throwaway containers (Testcontainers).

Session scope: containers start once per test run, the schema is built by the real Alembic
migrations. Function scope: every test starts with empty tables and an empty Redis."""

import asyncio
import os
from pathlib import Path

# Ryuk is Testcontainers' cleanup sidecar (an extra image pull); our fixtures stop containers
# themselves. Must be set before testcontainers is imported.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

import pytest
from alembic import command
from alembic.config import Config
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer

from app.config import Settings
from app.main import create_app

SERVICE_DIR = Path(__file__).resolve().parents[1]
APP_DB_PASSWORD = "identity-test-password"

# Same statements as infra/postgres/init.sh, so tests run with production-like privileges
SCHEMA_SETUP_SQL = f"""
CREATE ROLE identity_user LOGIN PASSWORD '{APP_DB_PASSWORD}';
CREATE SCHEMA identity AUTHORIZATION identity_user;
ALTER ROLE identity_user SET search_path = identity;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
"""


@pytest.fixture(scope="session")
def database_url():
    with PostgresContainer("postgres:16", driver=None) as pg:
        result = pg.exec(["psql", "-v", "ON_ERROR_STOP=1", "-U", pg.username, "-d", pg.dbname, "-c", SCHEMA_SETUP_SQL])
        assert result.exit_code == 0, result.output
        host, port = pg.get_container_host_ip(), pg.get_exposed_port(5432)
        yield f"postgresql+asyncpg://identity_user:{APP_DB_PASSWORD}@{host}:{port}/{pg.dbname}"


@pytest.fixture(scope="session")
def redis_url():
    with RedisContainer("redis:7") as redis:
        yield f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}/0"


@pytest.fixture(scope="session")
def private_key_path(tmp_path_factory) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    path = tmp_path_factory.mktemp("keys") / "jwt_private.pem"
    path.write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    return path


@pytest.fixture(scope="session")
def alembic_config(database_url, private_key_path) -> Config:
    # alembic/env.py builds Settings() from the environment, like in the container
    os.environ.update(
        DATABASE_URL=database_url, DB_SCHEMA="identity", JWT_PRIVATE_KEY_PATH=str(private_key_path)
    )
    cfg = Config(str(SERVICE_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(SERVICE_DIR / "alembic"))
    return cfg


@pytest.fixture(scope="session")
def migrated(alembic_config):
    command.upgrade(alembic_config, "head")
    return alembic_config


@pytest.fixture(scope="session")
def settings(migrated, database_url, redis_url, private_key_path) -> Settings:
    return Settings(
        database_url=database_url,
        redis_url=redis_url,
        jwt_private_key_path=private_key_path,
        login_max_failures_per_email=3,  # small limits keep the rate-limit tests short
        login_max_failures_per_ip=6,
        _env_file=None,
    )


async def _reset_state(database_url: str, redis_url: str) -> None:
    engine = create_async_engine(database_url)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE users, recruiter_profiles, refresh_tokens CASCADE"))
    await engine.dispose()
    from redis.asyncio import Redis

    redis = Redis.from_url(redis_url)
    await redis.flushdb()
    await redis.aclose()


def run(coro):
    """Run async setup code from a sync test (TestClient runs the app on its own loop)."""
    return asyncio.run(coro)


@pytest.fixture
def client(settings):
    run(_reset_state(settings.database_url, settings.redis_url))
    # https: the refresh cookie is Secure, so the client only sends it back over https
    with TestClient(create_app(settings), base_url="https://testserver") as c:
        yield c
