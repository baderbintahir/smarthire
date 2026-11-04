from redis.asyncio import Redis

# Module-level like the DB engine: one client (= one connection pool) per service process.
_redis: Redis | None = None


def init_redis(url: str, *, timeout: float = 2.0) -> Redis:
    """Call once at service startup, e.g. init_redis("redis://redis:6379/0")."""
    global _redis
    # decode_responses: get str back instead of bytes. Timeouts (Step 14.1): without them a Redis that
    # accepts connections but never answers (overloaded, network partition) would hang every request
    # that checks the denylist or an idempotency key; with them, the caller gets an error -> 503
    _redis = Redis.from_url(url, decode_responses=True, socket_timeout=timeout, socket_connect_timeout=timeout)
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
