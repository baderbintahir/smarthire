"""MongoDB helpers for services that store documents (Job Enrichment, Scoring, AI Assistant).
Needs the `mongo` extra: smarthire-common[mongo]. Module-level like db.py and redis.py: init at
startup, use anywhere. Uses PyMongo's own async API (AsyncMongoClient)."""

from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase

_client: AsyncMongoClient | None = None
_db: AsyncDatabase | None = None


def init_mongo(url: str, database: str) -> AsyncDatabase:
    """Call once at startup. Each service uses its own database (database per service)."""
    global _client, _db
    # Connecting is lazy: this doesn't fail if Mongo is down, the first query does
    _client = AsyncMongoClient(url, serverSelectionTimeoutMS=3000, tz_aware=True)
    _db = _client[database]
    return _db


def get_db() -> AsyncDatabase:
    if _db is None:
        raise RuntimeError("Mongo not initialised: call init_mongo() at startup")
    return _db


async def close_mongo() -> None:
    global _client, _db
    if _client is not None:
        await _client.close()
    _client, _db = None, None


async def mongo_ping_check() -> None:
    """Readiness check: the server answers."""
    await get_db().command("ping")
