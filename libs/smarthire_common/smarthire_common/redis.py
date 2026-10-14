from redis.asyncio import Redis

# Module-level like the DB engine: one client (= one connection pool) per service process.
_redis: Redis | None = None


def init_redis(url: str) -> Redis:
    """Call once at service startup, e.g. init_redis("redis://redis:6379/0")."""
    global _redis
    # decode_responses: get str back instead of bytes
    _redis = Redis.from_url(url, decode_responses=True)
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis is not None:
        await _redis.aclose()
    _redis = None


def get_redis() -> Redis:
    """Also usable as a FastAPI dependency: `redis: Redis = Depends(get_redis)`."""
    if _redis is None:
        raise RuntimeError("Redis not initialised: call init_redis() at startup")
    return _redis


async def redis_ping_check() -> None:
    await get_redis().ping()
