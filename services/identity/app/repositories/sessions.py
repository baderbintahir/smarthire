"""Refresh sessions in Redis (design v1.2, Appendix E.13). One session per login.

    identity:session:{sid}            hash: user_id, secret_hash, user_agent, created_at   TTL 14 d, renewed on refresh
    identity:user_sessions:{user_id}  set of the user's sids (logout-all, suspension)     TTL 14 d, renewed

The refresh token (cookie) is "{sid}.{secret}". Only SHA-256(secret) is stored: someone reading
Redis can't use what they see. Revoking a session = deleting its key; nothing expires early.
Only Identity reads or writes these keys."""

import time

from redis.asyncio import Redis

SESSION_PREFIX = "identity:session:"
USER_SESSIONS_PREFIX = "identity:user_sessions:"


def session_key(sid: str) -> str:
    return f"{SESSION_PREFIX}{sid}"


def user_sessions_key(user_id: str) -> str:
    return f"{USER_SESSIONS_PREFIX}{user_id}"


# Rotation must be atomic: two refreshes racing with the same cookie must not both win. A Lua
# script runs inside Redis with nothing else in between (like the old SELECT ... FOR UPDATE).
#   KEYS[1] session key   ARGV[1] hash of the presented secret   ARGV[2] hash of the new secret   ARGV[3] ttl
# Returns {status, user_id}: 0 = no such session, 1 = rotated, -1 = an OLD secret was presented
# (reuse: two parties hold the token), so the session is deleted for both.
_ROTATE = """
if redis.call('EXISTS', KEYS[1]) == 0 then return {0, ''} end
local user_id = redis.call('HGET', KEYS[1], 'user_id')
if redis.call('HGET', KEYS[1], 'secret_hash') ~= ARGV[1] then
  redis.call('DEL', KEYS[1])
  return {-1, user_id}
end
redis.call('HSET', KEYS[1], 'secret_hash', ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return {1, user_id}
"""

ROTATED, MISSING, REUSED = 1, 0, -1


async def create(redis: Redis, *, sid: str, user_id: str, secret_hash: str, user_agent: str | None, ttl_seconds: int) -> None:
    # transaction=True: MULTI/EXEC, both keys are written together or not at all
    async with redis.pipeline(transaction=True) as pipe:
        pipe.hset(session_key(sid), mapping={
            "user_id": user_id, "secret_hash": secret_hash,
            "user_agent": (user_agent or "")[:500], "created_at": str(int(time.time())),
        })
        pipe.expire(session_key(sid), ttl_seconds)
        pipe.sadd(user_sessions_key(user_id), sid)
        pipe.expire(user_sessions_key(user_id), ttl_seconds)  # outlives every session it lists
        await pipe.execute()


async def rotate(redis: Redis, *, sid: str, presented_hash: str, new_hash: str, ttl_seconds: int) -> tuple[int, str]:
    """-> (ROTATED | MISSING | REUSED, user_id). On ROTATED the presented secret is dead from now on."""
    status, user_id = await redis.eval(_ROTATE, 1, session_key(sid), presented_hash, new_hash, ttl_seconds)
    status = int(status)
    if status == ROTATED:
        await redis.expire(user_sessions_key(user_id), ttl_seconds)
    elif status == REUSED:
        await redis.srem(user_sessions_key(user_id), sid)  # the index entry; the session itself is gone
    return status, user_id


async def delete(redis: Redis, sid: str) -> None:
    """Ends one session. Unknown sid: nothing to do (logout is idempotent)."""
    user_id = await redis.hget(session_key(sid), "user_id")
    await redis.delete(session_key(sid))
    if user_id:
        await redis.srem(user_sessions_key(user_id), sid)


async def delete_all_for_user(redis: Redis, user_id: str) -> None:
    """Ends every session of the user (logout-all, suspension). Set members whose session already
    expired are deleted too: DEL on a missing key is a no-op."""
    sids = await redis.smembers(user_sessions_key(user_id))
    await redis.delete(user_sessions_key(user_id), *(session_key(sid) for sid in sids))
