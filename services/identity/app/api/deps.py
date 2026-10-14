"""FastAPI dependencies shared by Identity's routers.

Token verification itself is the shared chassis code (smarthire_common.auth), same as every
other service. Identity only adds checks that need its own users table."""

from fastapi import Depends, Request
from redis.asyncio import Redis
from smarthire_common import auth
from smarthire_common.auth import CurrentUser
from smarthire_common.db import get_session
from smarthire_common.errors import UnauthorizedError
from smarthire_common.redis import get_redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.domain.tokens import SigningKey
from app.repositories import users as users_repo
from app.services.login_rate_limiter import LoginRateLimiter

__all__ = ["CurrentUser", "get_current_user", "require_role"]


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_signing_key(request: Request) -> SigningKey:
    return request.app.state.signing_key


def get_login_rate_limiter(
    redis: Redis = Depends(get_redis), settings: Settings = Depends(get_settings)
) -> LoginRateLimiter:
    return LoginRateLimiter(redis, settings)


async def get_current_user(
    user: CurrentUser = Depends(auth.get_current_user),  # signature, exp, iss, aud, denylist
    session: AsyncSession = Depends(get_session),
) -> CurrentUser:
    # Identity-only extra checks (it owns the users table): account still active, and no
    # "logout everywhere" since the token was issued. Other services can't do this.
    db_user = await users_repo.get_by_id(session, user.user_id)
    if db_user is None or db_user.status != "ACTIVE" or db_user.token_version != user.claims.get("tv"):
        raise UnauthorizedError("Token is no longer valid")
    return user


def require_role(*roles: str):
    # The chassis role check, but built on Identity's stricter get_current_user
    return auth.require_role(*roles, user_dependency=get_current_user)
