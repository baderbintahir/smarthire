"""FastAPI dependencies shared by Identity's routers.

get_current_user verifies tokens LOCALLY with Identity's own key. Phase 3 replaces it with a
chassis version (JWKS client) that every service uses; keep the behaviour identical."""

import uuid
from dataclasses import dataclass

import jwt
from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.asyncio import Redis
from smarthire_common.db import get_session
from smarthire_common.errors import ForbiddenError, UnauthorizedError
from smarthire_common.redis import get_redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.domain.tokens import SigningKey, decode_access_token
from app.repositories import users as users_repo
from app.services.admin import is_revoked
from app.services.login_rate_limiter import LoginRateLimiter

# auto_error=False: we raise our own UnauthorizedError so the error body has our standard shape
_bearer = HTTPBearer(auto_error=False)


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_signing_key(request: Request) -> SigningKey:
    return request.app.state.signing_key


def get_login_rate_limiter(
    redis: Redis = Depends(get_redis), settings: Settings = Depends(get_settings)
) -> LoginRateLimiter:
    return LoginRateLimiter(redis, settings)


@dataclass(frozen=True)
class CurrentUser:
    user_id: uuid.UUID
    role: str
    jti: str


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
    key: SigningKey = Depends(get_signing_key),
    settings: Settings = Depends(get_settings),
    redis: Redis = Depends(get_redis),
    session: AsyncSession = Depends(get_session),
) -> CurrentUser:
    if credentials is None:
        raise UnauthorizedError("Missing bearer token")
    try:
        # 1. signature (made by our private key?), 2. exp, 3. iss, 4. aud
        claims = decode_access_token(
            credentials.credentials, key.public_key, issuer=settings.jwt_issuer, audience=settings.jwt_audience
        )
    except jwt.InvalidTokenError:
        # One message for every failure: don't tell an attacker which check failed
        raise UnauthorizedError("Invalid or expired token") from None

    # 5. not explicitly revoked by an admin (Redis denylist)
    if await is_revoked(redis, claims["jti"]):
        raise UnauthorizedError("Token has been revoked")

    # 6. Identity-only extra checks (it owns the users table): account still active, and no
    #    "logout everywhere" since the token was issued
    user = await users_repo.get_by_id(session, uuid.UUID(claims["sub"]))
    if user is None or user.status != "ACTIVE" or user.token_version != claims.get("tv"):
        raise UnauthorizedError("Token is no longer valid")

    return CurrentUser(user_id=user.id, role=user.role, jti=claims["jti"])


def require_role(*roles: str):
    """Usage: `user: CurrentUser = Depends(require_role("ADMIN"))`. 401 if not logged in
    (from get_current_user), 403 if logged in with the wrong role."""

    async def checker(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if user.role not in roles:
            raise ForbiddenError("You do not have permission to do this")
        return user

    return checker
