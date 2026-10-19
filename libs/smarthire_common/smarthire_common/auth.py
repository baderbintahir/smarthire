"""Token verification shared by every service (Phase 3).

Usage in a service:
    startup:   init_auth(JWKSClient(settings.jwks_url), issuer=..., audience=...)
    routes:    user: CurrentUser = Depends(get_current_user)
               user: CurrentUser = Depends(require_role("RECRUITER"))
               ensure_owner(job.recruiter_id, user)

401 = we don't know who you are (token missing/bad/expired/revoked).
403 = we know who you are, and you're not allowed (wrong role, not the owner).
503 = we can't check right now (keys or denylist unavailable). Fail CLOSED: never let an
      unchecked token through."""

import asyncio
import logging
import math
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx
import jwt
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.exceptions import RedisError

from smarthire_common.errors import ForbiddenError, ServiceUnavailableError, UnauthorizedError
from smarthire_common.redis import get_redis

logger = logging.getLogger(__name__)

ALGORITHM = "RS256"


def denylist_key(jti: str) -> str:
    """Appendix E.13: one revoked access token. Identity writes it; every verifier
    (gateway + services) reads it."""
    return f"denylist:{jti}"


def revoked_before_key(user_id: str) -> str:
    """Appendix E.13: reject this user's tokens issued AT OR BEFORE the stored unix time
    (account suspended, or "logout everywhere"). Identity writes it; every verifier reads it."""
    return f"revoked_before:{user_id}"


# ---------- where public keys come from ----------


class KeyProvider(Protocol):
    async def get_key(self, kid: str) -> Any | None: ...


class StaticKeyProvider:
    """Keys already in memory. Used by Identity (it owns the key; fetching its own JWKS over
    HTTP would be pointless) and by tests."""

    def __init__(self, keys: dict[str, Any]) -> None:
        self._keys = keys

    async def get_key(self, kid: str) -> Any | None:
        return self._keys.get(kid)


class JWKSClient:
    """Fetches Identity's public keys and caches them.

    - Cache for `cache_ttl` seconds (10 min): verifying a token needs no network call.
    - Unknown kid -> refetch immediately (Identity probably rotated its key)...
    - ...but at most once per `min_refresh_interval` (30 s), so garbage kids can't make every
      service hammer Identity.
    - If a refresh fails, keep using the old keys (Identity being briefly down shouldn't
      break every service)."""

    def __init__(
        self,
        url: str,
        *,
        http_client: httpx.AsyncClient | None = None,
        cache_ttl: float = 600,
        min_refresh_interval: float = 30,
        timeout: float = 3.0,
        clock: Callable[[], float] = time.monotonic,  # injectable so tests can move time
    ) -> None:
        self.url = url
        self._http = http_client or httpx.AsyncClient(timeout=timeout)
        self._owns_http = http_client is None
        self.cache_ttl = cache_ttl
        self.min_refresh_interval = min_refresh_interval
        self._clock = clock
        self._keys: dict[str, Any] = {}
        self._fetched_at: float | None = None
        self._last_attempt = -math.inf
        # Many requests may hit an unknown kid at once: only one of them fetches
        self._lock = asyncio.Lock()

    async def get_key(self, kid: str) -> Any | None:
        now = self._clock()
        stale = self._fetched_at is None or now - self._fetched_at >= self.cache_ttl
        if (stale or kid not in self._keys) and self._may_refresh(now):
            await self._refresh()
        if not self._keys:
            # Never managed to load any key: we can't tell good tokens from bad ones
            raise ServiceUnavailableError("Token verification is temporarily unavailable")
        return self._keys.get(kid)

    def _may_refresh(self, now: float) -> bool:
        return now - self._last_attempt >= self.min_refresh_interval

    async def _refresh(self) -> None:
        async with self._lock:
            now = self._clock()
            if not self._may_refresh(now):  # someone else refreshed while we waited for the lock
                return
            self._last_attempt = now
            try:
                response = await self._http.get(self.url)
                response.raise_for_status()
                keys = {
                    jwk["kid"]: jwt.PyJWK(jwk).key
                    for jwk in response.json()["keys"]
                    if jwk.get("kty") == "RSA" and jwk.get("kid")
                }
            except (httpx.HTTPError, ValueError, KeyError, jwt.PyJWKError):
                logger.warning("JWKS refresh from %s failed; keeping cached keys", self.url, exc_info=True)
                return
            self._keys, self._fetched_at = keys, now

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()


# ---------- module-level configuration (set once at startup, like init_db) ----------


@dataclass
class _AuthConfig:
    key_provider: KeyProvider
    issuer: str
    audience: str


_config: _AuthConfig | None = None


def init_auth(key_provider: KeyProvider, *, issuer: str, audience: str) -> None:
    global _config
    _config = _AuthConfig(key_provider, issuer, audience)


async def close_auth() -> None:
    global _config
    if _config is not None and isinstance(_config.key_provider, JWKSClient):
        await _config.key_provider.aclose()
    _config = None


def _get_config() -> _AuthConfig:
    if _config is None:
        raise RuntimeError("Auth not initialised: call init_auth() at startup")
    return _config


# ---------- verification ----------


async def verify_token(token: str) -> dict:
    """Returns the claims of a valid token. Raises UnauthorizedError (401) for any bad token,
    ServiceUnavailableError (503) if we can't check right now."""
    config = _get_config()
    # One generic message for every failure: don't tell an attacker which check failed
    invalid = UnauthorizedError("Invalid or expired token")

    try:
        kid = jwt.get_unverified_header(token).get("kid")  # header is unverified: only used to pick a key
    except jwt.InvalidTokenError:
        raise invalid from None
    key = await config.key_provider.get_key(kid) if kid else None
    if key is None:
        raise invalid

    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=[ALGORITHM],  # pinned: never trust the token's own "alg" header
            issuer=config.issuer,
            audience=config.audience,
            options={"require": ["exp", "iat", "sub", "jti", "iss", "aud", "role"]},
            leeway=10,  # tolerate 10 s of clock skew between machines
        )
    except jwt.InvalidTokenError:
        raise invalid from None

    try:
        # One round trip for both revocation keys
        denied, revoked_before = await get_redis().mget(
            [denylist_key(claims["jti"]), revoked_before_key(claims["sub"])]
        )
    except RedisError:
        # Fail CLOSED: if we can't check the denylist, a revoked token could slip through
        logger.exception("Denylist check failed")
        raise ServiceUnavailableError("Token verification is temporarily unavailable") from None
    # "<=": iat has 1-second resolution, so a token issued in the SAME second as the revocation
    # can't be told apart from one issued just before it; reject both (security over convenience:
    # logging in again within that same second needs one retry)
    if denied is not None or (revoked_before is not None and claims["iat"] <= int(revoked_before)):
        raise UnauthorizedError("Token has been revoked")
    return claims


# ---------- FastAPI dependencies ----------


@dataclass(frozen=True)
class CurrentUser:
    user_id: uuid.UUID
    role: str
    jti: str
    claims: dict = field(repr=False, compare=False)  # everything else, e.g. "tv" for Identity


# auto_error=False: we raise our own UnauthorizedError so the body has our standard shape
_bearer = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> CurrentUser:
    if credentials is None:
        raise UnauthorizedError("Missing bearer token")
    claims = await verify_token(credentials.credentials)
    try:
        user_id = uuid.UUID(claims["sub"])
    except ValueError:
        raise UnauthorizedError("Invalid or expired token") from None
    return CurrentUser(user_id=user_id, role=claims["role"], jti=claims["jti"], claims=claims)


def require_role(*roles: str, user_dependency: Callable = get_current_user):
    """`Depends(require_role("RECRUITER", "ADMIN"))`. user_dependency lets a service add its own
    checks on top of get_current_user (Identity does) and still reuse this."""

    async def checker(user: CurrentUser = Depends(user_dependency)) -> CurrentUser:
        if user.role not in roles:
            raise ForbiddenError("You do not have permission to do this")
        return user

    return checker


def ensure_owner(resource_owner_id: uuid.UUID, user: CurrentUser) -> None:
    """Step 3.2 ownership pattern. A role says what KIND of thing you may do ("recruiters edit
    jobs"); ownership says WHICH things ("only your own jobs"). Load the resource, then:

        job = await jobs_repo.get(session, job_id)
        ensure_owner(job.recruiter_id, user)       # 403 if it's someone else's
    """
    if resource_owner_id != user.user_id:
        raise ForbiddenError("You do not have access to this resource")
