"""Use cases: login, refresh, logout, logout-all (Steps 2.7-2.9; design v1.2: sessions in Redis).

A "session" = one login. It lives in Redis (app/repositories/sessions.py); the refresh cookie is
"{sid}.{secret}". Each refresh rotates the secret; presenting an old secret (reuse) deletes the
session; logout deletes it. Access tokens stay JWTs, verified locally everywhere."""

import logging
import uuid
from dataclasses import dataclass

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.domain import passwords, tokens
from app.domain.errors import (
    AccountSuspendedError,
    InvalidCredentialsError,
    InvalidRefreshTokenError,
)
from app.models import User
from app.repositories import sessions as sessions_repo
from app.repositories import users as users_repo
from app.services import admin
from app.services.login_rate_limiter import LoginRateLimiter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IssuedTokens:
    access_token: str
    expires_in: int
    refresh_token: str  # plain "{sid}.{secret}": goes into the cookie, only the secret's hash is stored


def _ttl_seconds(settings: Settings) -> int:
    return settings.refresh_token_ttl_days * 24 * 3600


async def _access_token(session: AsyncSession, user: User, *, key: tokens.SigningKey, settings: Settings) -> str:
    access_token, _ = tokens.create_access_token(
        key,
        user_id=user.id,
        # Read fresh from user_roles at every login/refresh: a granted role appears in the next token
        roles=await users_repo.get_roles(session, user.id),
        name=user.full_name,
        token_version=user.token_version,
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        ttl_seconds=settings.access_token_ttl_seconds,
    )
    return access_token


async def login(
    session: AsyncSession,
    redis: Redis,
    limiter: LoginRateLimiter,
    *,
    email: str,
    password: str,
    ip: str,
    user_agent: str | None,
    key: tokens.SigningKey,
    settings: Settings,
) -> IssuedTokens:
    email = email.strip().lower()
    await limiter.ensure_allowed(email, ip)

    user = await users_repo.get_by_email(session, email)
    # Always run one full Argon2 verify, even for unknown emails (DUMMY_HASH), so response time
    # doesn't reveal which emails are registered
    password_ok = passwords.verify_password(user.password_hash if user else passwords.DUMMY_HASH, password)
    if user is None or not password_ok:
        await limiter.record_failure(email, ip)
        raise InvalidCredentialsError()
    if user.status != "ACTIVE":
        raise AccountSuspendedError()

    await limiter.reset(email)
    if passwords.needs_rehash(user.password_hash):
        await users_repo.set_password_hash(session, user, passwords.hash_password(password))
        await session.commit()

    sid, secret = tokens.new_session_id(), tokens.new_refresh_secret()
    # Saved BEFORE the response: the client must never get a refresh token we didn't store
    await sessions_repo.create(redis, sid=sid, user_id=str(user.id), secret_hash=tokens.hash_refresh_secret(secret),
                               user_agent=user_agent, ttl_seconds=_ttl_seconds(settings))
    access_token = await _access_token(session, user, key=key, settings=settings)
    return IssuedTokens(access_token, settings.access_token_ttl_seconds, tokens.format_refresh_token(sid, secret))


async def refresh(
    session: AsyncSession,
    redis: Redis,
    *,
    refresh_token: str | None,
    key: tokens.SigningKey,
    settings: Settings,
) -> IssuedTokens:
    parsed = tokens.parse_refresh_token(refresh_token)
    if parsed is None:
        raise InvalidRefreshTokenError()
    sid, secret = parsed
    new_secret = tokens.new_refresh_secret()
    status, user_id = await sessions_repo.rotate(
        redis, sid=sid, presented_hash=tokens.hash_refresh_secret(secret),
        new_hash=tokens.hash_refresh_secret(new_secret), ttl_seconds=_ttl_seconds(settings),
    )
    if status == sessions_repo.REUSED:
        # REUSE DETECTED. This secret was already exchanged once, so two parties hold it: the real
        # user and (probably) a thief. We can't tell which one is calling; the script ended the
        # session for both.
        logger.warning("Refresh token reuse detected; session %s of user %s deleted", sid, user_id)
        raise InvalidRefreshTokenError()
    if status == sessions_repo.MISSING:  # logged out, expired, revoked, or never existed
        raise InvalidRefreshTokenError()

    user = await users_repo.get_by_id(session, uuid.UUID(user_id))
    if user is None or user.status != "ACTIVE":
        await sessions_repo.delete(redis, sid)  # e.g. suspended while a login raced the suspension
        raise InvalidRefreshTokenError()
    access_token = await _access_token(session, user, key=key, settings=settings)
    return IssuedTokens(access_token, settings.access_token_ttl_seconds, tokens.format_refresh_token(sid, new_secret))


async def logout(redis: Redis, *, refresh_token: str | None) -> None:
    """Ends this one session. Idempotent: no cookie, a malformed one or an unknown session is fine."""
    parsed = tokens.parse_refresh_token(refresh_token)
    if parsed is not None:
        await sessions_repo.delete(redis, parsed[0])


async def logout_all(session: AsyncSession, redis: Redis, *, user_id: uuid.UUID, settings: Settings) -> None:
    """Ends every session of the user (Redis) and revokes their current access tokens everywhere:
    token_version for Identity, `revoked_before` in Redis for the gateway and services."""
    await sessions_repo.delete_all_for_user(redis, str(user_id))
    await users_repo.bump_token_version(session, user_id)
    await admin.revoke_all_access_tokens(redis, user_id=user_id, ttl_seconds=settings.access_token_ttl_seconds)
    await session.commit()
