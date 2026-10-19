"""Shared test infrastructure for services (used from each service's tests/conftest.py).

Real Postgres/Redis in throwaway containers (Testcontainers), set up exactly like production:
the service gets its own DB user and schema (same statements as infra/postgres/init.sh), and the
schema is built by the service's real Alembic migrations.

Needs `testcontainers[postgres,redis]` and `alembic`, which services have as (dev) dependencies;
they're imported lazily so production images never need them."""

import asyncio
import os
from collections.abc import Coroutine, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

# Ryuk is Testcontainers' cleanup sidecar (an extra image pull); our context managers stop the
# containers themselves. Must be set before testcontainers is imported.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

POSTGRES_IMAGE = "postgres:16"
REDIS_IMAGE = "redis:7"


def _schema_setup_sql(service: str, password: str) -> str:
    # Mirrors infra/postgres/init.sh: own login role, own schema, search_path, no access to public
    return f"""
    CREATE ROLE {service}_user LOGIN PASSWORD '{password}';
    CREATE SCHEMA {service} AUTHORIZATION {service}_user;
    ALTER ROLE {service}_user SET search_path = {service};
    REVOKE ALL ON SCHEMA public FROM PUBLIC;
    """


@contextmanager
def service_postgres(service: str) -> Iterator[str]:
    """Starts Postgres with the service's user + schema. Yields the asyncpg URL for that user."""
    from testcontainers.community.postgres import PostgresContainer

    password = f"{service}-test-password"
    with PostgresContainer(POSTGRES_IMAGE, driver=None) as pg:
        result = pg.exec(
            ["psql", "-v", "ON_ERROR_STOP=1", "-U", pg.username, "-d", pg.dbname,
             "-c", _schema_setup_sql(service, password)]
        )
        assert result.exit_code == 0, result.output
        host, port = pg.get_container_host_ip(), pg.get_exposed_port(5432)
        yield f"postgresql+asyncpg://{service}_user:{password}@{host}:{port}/{pg.dbname}"


@contextmanager
def redis_container() -> Iterator[str]:
    """Starts Redis. Yields its URL."""
    from testcontainers.community.redis import RedisContainer

    with RedisContainer(REDIS_IMAGE) as redis:
        yield f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}/0"


def alembic_config(service_dir: Path, *, database_url: str, schema: str, extra_env: dict[str, str] | None = None):
    """An Alembic Config for the service's migrations. alembic/env.py builds the service's Settings()
    from the environment (as in the container), so the needed variables are set here."""
    from alembic.config import Config

    os.environ.update(DATABASE_URL=database_url, DB_SCHEMA=schema, **(extra_env or {}))
    cfg = Config(str(service_dir / "alembic.ini"))
    cfg.set_main_option("script_location", str(service_dir / "alembic"))
    return cfg


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    """Run async setup code from a sync test or fixture (TestClient runs the app on its own loop)."""
    return asyncio.run(coro)


async def truncate(database_url: str, tables: Sequence[str]) -> None:
    """Empties the given tables. Uses its own engine: never call init_db()/close_db() from tests
    while an app is running, they swap the process-wide engine the app uses."""
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {', '.join(tables)} CASCADE"))
    await engine.dispose()


async def flush_redis(redis_url: str) -> None:
    from redis.asyncio import Redis

    redis = Redis.from_url(redis_url)
    await redis.flushdb()
    await redis.aclose()


async def schema_object_names(database_url: str, schema: str) -> set[str]:
    """Every constraint and index name in the schema (Alembic's own table excluded). Tests compare
    this to Appendix E EXACTLY: `alembic check` doesn't compare CHECK constraints, and an "is the
    name in the error message" test can't tell `ck_jobs_status` from `ck_jobs_ck_jobs_status`."""
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                """
                SELECT c.conname FROM pg_constraint c
                  JOIN pg_namespace n ON n.oid = c.connamespace
                  JOIN pg_class t ON t.oid = c.conrelid
                 WHERE n.nspname = :schema AND t.relname <> 'alembic_version'
                UNION
                SELECT indexname FROM pg_indexes
                 WHERE schemaname = :schema AND tablename <> 'alembic_version'
                """
            ),
            {"schema": schema},
        )
        names = {row[0] for row in rows}
    await engine.dispose()
    return names


class TokenFactory:
    """Mints valid access tokens for tests, signed by a throwaway key, so a service can test its
    auth rules without running Identity. Pass `key_provider` to the service's create_app().

        tokens = TokenFactory()
        app = create_app(settings, key_provider=tokens.key_provider)
        headers = tokens.headers(role="RECRUITER")      # a fresh user
        headers = tokens.headers(user_id=same_id)        # the same user again
    """

    KID = "test-key"

    def __init__(self, issuer: str = "smarthire-identity", audience: str = "smarthire") -> None:
        from cryptography.hazmat.primitives.asymmetric import rsa

        from smarthire_common.auth import StaticKeyProvider

        self._key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.key_provider = StaticKeyProvider({self.KID: self._key.public_key()})
        self.issuer, self.audience = issuer, audience

    def mint(self, *, user_id=None, role: str = "CANDIDATE", ttl_seconds: int = 900, **claims) -> str:
        import time
        import uuid

        import jwt

        now = int(time.time())
        payload = {
            "sub": str(user_id or uuid.uuid4()), "role": role, "iss": self.issuer, "aud": self.audience,
            "iat": now, "exp": now + ttl_seconds, "jti": str(uuid.uuid4()), "tv": 1, **claims,
        }
        return jwt.encode(payload, self._key, algorithm="RS256", headers={"kid": self.KID})

    def headers(self, **kwargs) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.mint(**kwargs)}"}
