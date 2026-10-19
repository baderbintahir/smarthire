"""Use cases: login, refresh, logout, logout-all (Steps 2.7-2.9).

A "session" here = one refresh-token family. Login starts a family; each refresh rotates to a new
token in the same family; logout or detected reuse revokes the family."""

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession
from smarthire_common.models import new_id

from app.config import Settings
from app.domain import passwords, tokens
from app.domain.errors import (
    AccountSuspendedError,
    InvalidCredentialsError,
    InvalidRefreshTokenError,
)
from app.models import RefreshToken, User
from app.repositories import refresh_tokens as tokens_repo
from app.repositories import users as users_repo
from app.services import admin
from app.services.login_rate_limiter import LoginRateLimiter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IssuedTokens:
    access_token: str
    expires_in: int
    refresh_token: str  # plain value: goes into the cookie, only its hash is stored


async def _issue_tokens(
    session: AsyncSession,
    user: User,
    *,
    family_id: uuid.UUID,
    user_agent: str | None,
    key: tokens.SigningKey,
    settings: Settings,
    now: datetime,
) -> IssuedTokens:
    access_token, _ = tokens.create_access_token(
        key,
        user_id=user.id,
        role=user.role,
        token_version=user.token_version,
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        ttl_seconds=settings.access_token_ttl_seconds,
        now=now,
    )
    refresh_plain = tokens.new_refresh_token()
    await tokens_repo.add(
        session,
        RefreshToken(
            id=new_id(),
            user_id=user.id,
            family_id=family_id,
            token_hash=tokens.hash_refresh_token(refresh_plain),
            user_agent=(user_agent or "")[:500] or None,
            expires_at=now + timedelta(days=settings.refresh_token_ttl_days),
        ),
    )
    return IssuedTokens(access_token, settings.access_token_ttl_seconds, refresh_plain)


async def login(
    session: AsyncSession,
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

    issued = await _issue_tokens(
        session, user, family_id=new_id(), user_agent=user_agent, key=key, settings=settings,
        now=datetime.now(UTC),
    )
    await session.commit()  # before the response: the client must never get a token we didn't save
    return issued


async def refresh(
    session: AsyncSession,
    *,
    refresh_token: str | None,
    user_agent: str | None,
    key: tokens.SigningKey,
    settings: Settings,
) -> IssuedTokens:
    if not refresh_token:
        raise InvalidRefreshTokenError()
    now = datetime.now(UTC)
    row = await tokens_repo.get_by_hash(session, tokens.hash_refresh_token(refresh_token), for_update=True)
    if row is None or row.revoked_at is not None:
        raise InvalidRefreshTokenError()

    if row.used_at is not None:
        # REUSE DETECTED. This token was already exchanged once, so two parties hold it: the
        # real user and (probably) a thief. We can't tell which one is calling, so we end the
        # whole session for both.
        await tokens_repo.revoke_family(session, row.family_id, now)
        # Commit explicitly: raising below makes session_scope roll back, and the revocation
        # must survive that
        await session.commit()
        logger.warning("Refresh token reuse detected; revoked family %s", row.family_id)
        raise InvalidRefreshTokenError()

    if row.expires_at <= now:
        raise InvalidRefreshTokenError()
    user = await users_repo.get_by_id(session, row.user_id)
    if user is None or user.status != "ACTIVE":
        raise InvalidRefreshTokenError()

    row.used_at = now  # rotation: this token can never be used again
    issued = await _issue_tokens(
        session, user, family_id=row.family_id, user_agent=user_agent, key=key, settings=settings, now=now
    )
    await session.commit()
    return issued


async def logout(session: AsyncSession, *, refresh_token: str | None) -> None:
    """Ends this one session (its whole family). Idempotent: no cookie or unknown token is fine."""
    if not refresh_token:
        return
    row = await tokens_repo.get_by_hash(session, tokens.hash_refresh_token(refresh_token))
    if row is not None:
        await tokens_repo.revoke_family(session, row.family_id, datetime.now(UTC))
        await session.commit()


async def logout_all(session: AsyncSession, redis: Redis, *, user_id: uuid.UUID, settings: Settings) -> None:
    """Ends every session of the user (refresh tokens) and revokes their current access tokens
    everywhere: token_version for Identity, `revoked_before` in Redis for the gateway and services."""
    await tokens_repo.revoke_all_for_user(session, user_id, datetime.now(UTC))
    await users_repo.bump_token_version(session, user_id)
    await admin.revoke_all_access_tokens(redis, user_id=user_id, ttl_seconds=settings.access_token_ttl_seconds)
    await session.commit()
