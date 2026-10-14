"""Use case: immediate revocation of one access token (Step 2.9)."""

from redis.asyncio import Redis


def denylist_key(jti: str) -> str:
    return f"denylist:{jti}"  # Appendix E.13


async def revoke_access_token(redis: Redis, *, jti: str, ttl_seconds: int) -> None:
    """The key only needs to live as long as the token could still be valid. We don't store
    issued tokens, so we don't know this token's exact exp; the maximum access-token lifetime is
    a safe upper bound (the token is certainly expired by then)."""
    await redis.set(denylist_key(jti), "1", ex=ttl_seconds)


async def is_revoked(redis: Redis, jti: str) -> bool:
    return bool(await redis.exists(denylist_key(jti)))
