"""HTTP layer for /auth. Only HTTP concerns here: parse input, call a use case, shape the
response, set/clear the cookie. Rules live in app/services and app/domain."""

from fastapi import APIRouter, Depends, Request, Response, status
from redis.asyncio import Redis
from smarthire_common.db import get_session
from smarthire_common.redis import get_redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import (
    CurrentUser,
    get_current_user,
    get_login_rate_limiter,
    get_settings,
    get_signing_key,
    require_role,
)
from app.api.users import to_user_out
from app.config import Settings
from app.domain.tokens import SigningKey
from app.schemas.auth import LoginRequest, RegisterRequest, TokenResponse, UserOut
from app.services import admin, registration, sessions
from app.services.login_rate_limiter import LoginRateLimiter

router = APIRouter(prefix="/auth", tags=["auth"])


def _set_refresh_cookie(response: Response, token: str, settings: Settings) -> None:
    response.set_cookie(
        settings.refresh_cookie_name,
        token,
        max_age=settings.refresh_token_ttl_days * 24 * 3600,
        httponly=True,  # JavaScript can't read it, so an XSS bug can't steal it
        secure=settings.refresh_cookie_secure,  # HTTPS only
        samesite="strict",  # not sent on requests started by other sites (CSRF protection)
        path=settings.refresh_cookie_path,  # only sent to /auth/*, not to every endpoint
    )


def _clear_refresh_cookie(response: Response, settings: Settings) -> None:
    response.delete_cookie(
        settings.refresh_cookie_name,
        path=settings.refresh_cookie_path,
        secure=settings.refresh_cookie_secure,
        httponly=True,
        samesite="strict",
    )


@router.post("/register", status_code=status.HTTP_201_CREATED, response_model=UserOut)
async def register(body: RegisterRequest, session: AsyncSession = Depends(get_session)):
    user, profile = await registration.register_user(
        session,
        email=body.email,
        password=body.password,
        role=body.role,
        display_name=body.display_name,
        company=body.company,
    )
    return to_user_out(user, profile)


@router.post("/login", response_model=TokenResponse)
async def login(
    body: LoginRequest,
    request: Request,
    response: Response,
    session: AsyncSession = Depends(get_session),
    limiter: LoginRateLimiter = Depends(get_login_rate_limiter),
    key: SigningKey = Depends(get_signing_key),
    settings: Settings = Depends(get_settings),
):
    issued = await sessions.login(
        session,
        limiter,
        email=body.email,
        password=body.password,
        # Direct client IP. Behind the gateway (Phase 4) this becomes the gateway's IP and we
        # must read X-Forwarded-For from the trusted gateway instead.
        ip=request.client.host if request.client else "unknown",
        user_agent=request.headers.get("user-agent"),
        key=key,
        settings=settings,
    )
    # Access token in the body (client keeps it in memory); refresh token only in the cookie
    _set_refresh_cookie(response, issued.refresh_token, settings)
    return TokenResponse(access_token=issued.access_token, expires_in=issued.expires_in)


@router.post("/refresh", response_model=TokenResponse)
async def refresh(
    request: Request,
    response: Response,
    session: AsyncSession = Depends(get_session),
    key: SigningKey = Depends(get_signing_key),
    settings: Settings = Depends(get_settings),
):
    issued = await sessions.refresh(
        session,
        refresh_token=request.cookies.get(settings.refresh_cookie_name),
        user_agent=request.headers.get("user-agent"),
        key=key,
        settings=settings,
    )
    _set_refresh_cookie(response, issued.refresh_token, settings)  # rotated: the old one is now used
    return TokenResponse(access_token=issued.access_token, expires_in=issued.expires_in)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    request: Request,
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
):
    # No access token required: logging out must work even after the access token expired
    await sessions.logout(session, refresh_token=request.cookies.get(settings.refresh_cookie_name))
    response = Response(status_code=status.HTTP_204_NO_CONTENT)
    _clear_refresh_cookie(response, settings)
    return response


@router.post("/logout-all", status_code=status.HTTP_204_NO_CONTENT)
async def logout_all(
    user: CurrentUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
):
    await sessions.logout_all(session, user_id=user.user_id)
    response = Response(status_code=status.HTTP_204_NO_CONTENT)
    _clear_refresh_cookie(response, settings)
    return response


@router.post("/revoke/{jti}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_token(
    jti: str,
    _admin: CurrentUser = Depends(require_role("ADMIN")),
    redis: Redis = Depends(get_redis),
    settings: Settings = Depends(get_settings),
):
    await admin.revoke_access_token(redis, jti=jti, ttl_seconds=settings.access_token_ttl_seconds)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
