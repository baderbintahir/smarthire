"""Test infrastructure: real Postgres + Redis in throwaway containers, schema built by the real
migrations, tokens from a test key, and a fake Candidate API. Every test starts empty."""

from decimal import Decimal
from pathlib import Path

import jwt
import pytest
from alembic import command
from smarthire_common import testing
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.domain.applications import CandidateFacts

SERVICE_DIR = Path(__file__).resolve().parents[1]
TABLES = ["job_replica", "applications", "application_stage_history", "application_notes", "application_quotas",
          "audit_entries", "outbox", "processed_events"]
MAX_ACTIVE = 3  # small, so quota tests stay short


@pytest.fixture(scope="session")
def database_url():
    with testing.service_postgres("application") as url:
        yield url


@pytest.fixture(scope="session")
def migrated(database_url):
    cfg = testing.alembic_config(SERVICE_DIR, database_url=database_url, schema="application")
    command.upgrade(cfg, "head")
    return cfg


@pytest.fixture
async def session(migrated, database_url):
    """A session connected as application_user, on freshly emptied tables."""
    await testing.truncate(database_url, TABLES)
    engine = create_async_engine(database_url)
    async with AsyncSession(engine, expire_on_commit=False) as s:
        yield s
    await engine.dispose()


# ---------- API tests ----------


class FakeCandidates:
    """Stands in for Candidate's API. Every user has a complete profile unless a test says otherwise."""

    def __init__(self) -> None:
        self.profiles: dict[str, CandidateFacts] = {}
        self.down = False
        self.calls = 0

    def set(self, user_id, **facts) -> None:
        defaults = {"full_name": "Jane Doe", "experience_years": Decimal("5"), "has_resume": True}
        self.profiles[str(user_id)] = CandidateFacts(**{**defaults, **facts})

    async def facts(self, authorization: str) -> CandidateFacts:
        from smarthire_common.errors import ServiceUnavailableError

        self.calls += 1
        if self.down:
            raise ServiceUnavailableError("Candidate profiles are unavailable right now; try again shortly")
        # Like the real service: whose profile is decided by the token (already verified by the app)
        user_id = jwt.decode(authorization.removeprefix("Bearer "), options={"verify_signature": False})["sub"]
        return self.profiles.get(user_id) or CandidateFacts("Jane Doe", Decimal("5"), True)

    async def close(self) -> None:
        pass


@pytest.fixture(scope="session")
def redis_url():
    with testing.redis_container() as url:
        yield url


@pytest.fixture(scope="session")
def tokens():
    return testing.TokenFactory()


@pytest.fixture
def candidates():
    return FakeCandidates()


@pytest.fixture
def client(migrated, database_url, redis_url, tokens, candidates):
    from fastapi.testclient import TestClient

    from app.config import Settings
    from app.main import create_app

    testing.run(testing.truncate(database_url, TABLES))
    testing.run(testing.flush_redis(redis_url))
    settings = Settings(database_url=database_url, redis_url=redis_url, max_active_applications=MAX_ACTIVE,
                        kafka_enabled=False, _env_file=None)
    with TestClient(create_app(settings, key_provider=tokens.key_provider, candidate_client=candidates)) as c:
        yield c
