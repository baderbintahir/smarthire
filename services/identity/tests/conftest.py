"""Test infrastructure: real Postgres + Redis in throwaway containers (smarthire_common.testing).

Session scope: containers start once per test run, the schema is built by the real Alembic
migrations. Function scope: every test starts with empty tables and an empty Redis."""

from pathlib import Path

import pytest
from alembic import command
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from smarthire_common import testing
from smarthire_common.testing import run  # noqa: F401  (re-exported for the tests)

from app.config import Settings
from app.main import create_app

SERVICE_DIR = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def database_url():
    with testing.service_postgres("identity") as url:
        yield url


@pytest.fixture(scope="session")
def redis_url():
    with testing.redis_container() as url:
        yield url


@pytest.fixture(scope="session")
def private_key_path(tmp_path_factory) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    path = tmp_path_factory.mktemp("keys") / "jwt_private.pem"
    path.write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    return path


@pytest.fixture(scope="session")
def migrated(database_url, private_key_path):
    cfg = testing.alembic_config(
        SERVICE_DIR,
        database_url=database_url,
        schema="identity",
        extra_env={"JWT_PRIVATE_KEY_PATH": str(private_key_path)},  # required by Identity's Settings
    )
    command.upgrade(cfg, "head")
    return cfg


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


@pytest.fixture
def client(settings):
    run(testing.truncate(settings.database_url, ["users", "recruiter_profiles", "refresh_tokens"]))
    run(testing.flush_redis(settings.redis_url))
    # https: the refresh cookie is Secure, so the client only sends it back over https
    with TestClient(create_app(settings), base_url="https://testserver") as c:
        yield c
