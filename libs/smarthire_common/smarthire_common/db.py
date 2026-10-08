from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# Module-level on purpose: one engine (= one connection pool) per service process.
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def init_db(database_url: str, **engine_kwargs) -> AsyncEngine:
    """Call once at service startup. Extra kwargs (pool_size, max_overflow, ...) go to
    create_async_engine; they are tuned per service in Phase 14."""
    global _engine, _session_factory
    # pool_pre_ping: test a pooled connection before using it, so a DB restart doesn't
    # cause one failed request per stale connection
    engine_kwargs.setdefault("pool_pre_ping", True)
    _engine = create_async_engine(database_url, **engine_kwargs)
    # expire_on_commit=False: objects stay readable after commit (lazy reloads
    # would need another await, which async code can't do implicitly)
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


async def close_db() -> None:
    """Call on service shutdown."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("Database not initialised: call init_db() at startup")
    return _engine


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """One transaction: commit if the block finishes, roll back if it raises.
    Also used outside HTTP (outbox relay, Kafka consumers)."""
    if _session_factory is None:
        raise RuntimeError("Database not initialised: call init_db() at startup")
    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except BaseException:
            await session.rollback()
            raise


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: `session: AsyncSession = Depends(get_session)`.
    Rolls back if the endpoint raises, and commits as a safety net when it returns.

    CAVEAT (verified on FastAPI 0.143): this post-yield commit runs AFTER the response has
    been sent. A constraint violation first raised at commit time would reach the client
    as a success. So any use case that must report DB errors (e.g. duplicate email -> 409)
    must `await session.commit()` itself before returning; the later commit is then a no-op."""
    async with session_scope() as session:
        yield session
