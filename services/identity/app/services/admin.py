"""Use cases: revoking access tokens (Step 2.9) and suspending users (Phase 4.3, Appendix F.3).

Identity is the only WRITER of the revocation keys in Redis; the gateway (APISIX plugin) and
every service (chassis verify_token) READ them, so a revoked token is stopped at the edge."""

import time
import uuid
from datetime import UTC, datetime

from redis.asyncio import Redis
from redis.exceptions import RedisError
from smarthire_common.auth import denylist_key, revoked_before_key  # one key format for all
from smarthire_common.errors import ConflictError, NotFoundError, ServiceUnavailableError
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import refresh_tokens as tokens_repo
from app.repositories import users as users_repo

# Revocation keys must outlive every token they cover: max token lifetime + verifier leeway
_LEEWAY_SECONDS = 60


async def revoke_access_token(redis: Redis, *, jti: str, ttl_seconds: int) -> None:
    """The key only needs to live as long as the token could still be valid. We don't store
    issued tokens, so we don't know this token's exact exp; the maximum access-token lifetime is
    a safe upper bound (the token is certainly expired by then)."""
    await redis.set(denylist_key(jti), "1", ex=ttl_seconds)


async def revoke_all_access_tokens(redis: Redis, *, user_id: uuid.UUID, ttl_seconds: int) -> None:
    """Every access token of this user issued before now is rejected by the gateway and by every
    service. After the max token lifetime they have all expired anyway, so the key can go too."""
    try:
        await redis.set(revoked_before_key(str(user_id)), str(int(time.time())), ex=ttl_seconds + _LEEWAY_SECONDS)
    except RedisError:
        raise ServiceUnavailableError("Could not revoke tokens right now; try again") from None


async def suspend_user(
    session: AsyncSession, redis: Redis, *, user_id: uuid.UUID, admin_id: uuid.UUID, ttl_seconds: int
) -> None:
    if user_id == admin_id:
        raise ConflictError("You cannot suspend your own account", code="CANNOT_SUSPEND_SELF")
    user = await users_repo.get_by_id(session, user_id)
    if user is None:
        raise NotFoundError("User not found")
    await users_repo.set_status(session, user_id, "SUSPENDED")
    await tokens_repo.revoke_all_for_user(session, user_id, datetime.now(UTC))  # no new access tokens
    await users_repo.bump_token_version(session, user_id)
    # Redis BEFORE commit: if the commit then fails, the tokens are revoked but the user isn't
    # suspended (safe direction). The other order could leave a suspended user with live tokens.
    await revoke_all_access_tokens(redis, user_id=user_id, ttl_seconds=ttl_seconds)
    await session.commit()
