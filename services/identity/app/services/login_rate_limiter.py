"""Failed-login counters in Redis (Step 2.10, Appendix E.13).

Two counters, because attackers come in two shapes:
- login_fail:{email}  - many guesses at ONE account (from anywhere)
- login_fail_ip:{ip}  - one machine trying MANY accounts (credential stuffing)
Each counter expires a fixed window after its first failure."""

from redis.asyncio import Redis
from smarthire_common.errors import TooManyRequestsError

from app.config import Settings


class LoginRateLimiter:
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self.redis = redis
        self.settings = settings

    @staticmethod
    def _email_key(email: str) -> str:
        return f"login_fail:{email}"

    @staticmethod
    def _ip_key(ip: str) -> str:
        return f"login_fail_ip:{ip}"

    async def ensure_allowed(self, email: str, ip: str) -> None:
        """Checked BEFORE the password is verified, so a blocked attacker learns nothing more."""
        limits = (
            (self._email_key(email), self.settings.login_max_failures_per_email),
            (self._ip_key(ip), self.settings.login_max_failures_per_ip),
        )
        counts = await self.redis.mget([key for key, _ in limits])
        for (key, limit), count in zip(limits, counts, strict=True):
            if count is not None and int(count) >= limit:
                retry_after = max(await self.redis.ttl(key), 1)
                raise TooManyRequestsError(
                    "Too many failed login attempts; try again later", retry_after=retry_after
                )

    async def record_failure(self, email: str, ip: str) -> None:
        window = self.settings.login_failure_window_seconds
        # One round trip, applied atomically (MULTI/EXEC)
        async with self.redis.pipeline(transaction=True) as pipe:
            for key in (self._email_key(email), self._ip_key(ip)):
                pipe.incr(key)
                # nx=True: set the expiry only on the first failure, so the window is fixed and
                # an attacker can't keep it alive forever (requires Redis 7)
                pipe.expire(key, window, nx=True)
            await pipe.execute()

    async def reset(self, email: str) -> None:
        # A successful login clears the account's counter (the IP counter keeps counting)
        await self.redis.delete(self._email_key(email))
