"""Idempotency keys for POST /applications (step 1 and 7 of the apply flow).

A client that times out can't know whether its apply went through. With an Idempotency-Key it just
sends the same request again: the first one's response comes back instead of a second application
(or a confusing 409). Stored in Redis as idem:{user_id}:{key} (E.13), 24 h.

States of a key:
  pending   - a request with this key is running right now (short TTL: if the process dies
              mid-request, the key frees itself after a minute instead of blocking for 24 h)
  done      - finished; the stored response is replayed
A key reused with a DIFFERENT body (another job) is an error, not a replay."""

import json

from redis.asyncio import Redis

from app.domain.errors import IdempotencyInProgressError, IdempotencyKeyReusedError

PENDING_TTL_SECONDS = 60


class IdempotencyStore:
    def __init__(self, redis: Redis, *, ttl_seconds: int) -> None:
        self._redis = redis
        self._ttl = ttl_seconds

    @staticmethod
    def _key(user_id, key: str) -> str:
        return f"idem:{user_id}:{key}"  # per user: two users can't collide on (or read) each other's keys

    async def begin(self, user_id, key: str, fingerprint: str) -> dict | None:
        """None = we own the key, go ahead. A dict = the stored response to replay."""
        marker = json.dumps({"state": "pending", "fingerprint": fingerprint})
        # SET NX: only one request can claim the key; the atomic check-and-set happens in Redis
        if await self._redis.set(self._key(user_id, key), marker, nx=True, ex=PENDING_TTL_SECONDS):
            return None
        raw = await self._redis.get(self._key(user_id, key))
        if raw is None:  # expired between our two calls: try once more
            return await self.begin(user_id, key, fingerprint)
        stored = json.loads(raw)
        if stored["fingerprint"] != fingerprint:
            raise IdempotencyKeyReusedError()
        if stored["state"] == "pending":
            raise IdempotencyInProgressError()
        return stored["response"]

    async def complete(self, user_id, key: str, fingerprint: str, response: dict) -> None:
        value = json.dumps({"state": "done", "fingerprint": fingerprint, "response": response})
        await self._redis.set(self._key(user_id, key), value, ex=self._ttl)

    async def release(self, user_id, key: str) -> None:
        """The request failed (422, 409, ...): free the key so the client can fix and retry with it."""
        await self._redis.delete(self._key(user_id, key))
