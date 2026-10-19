import uuid

from fastapi import APIRouter, Depends, Response, status
from redis.asyncio import Redis
from smarthire_common.db import get_session
from smarthire_common.redis import get_redis
from smarthire_common.errors import NotFoundError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_current_user, get_settings, require_role
from app.config import Settings
from app.models import RecruiterProfile, User
from app.repositories import users as users_repo
from app.schemas.auth import RecruiterProfileOut, UserOut
from app.services import admin

router = APIRouter(prefix="/users", tags=["users"])


def to_user_out(user: User, profile: RecruiterProfile | None) -> UserOut:
    return UserOut(
        id=user.id,
        email=user.email,
        role=user.role,
        status=user.status,
        created_at=user.created_at,
        recruiter_profile=(
            RecruiterProfileOut(display_name=profile.display_name, company=profile.company)
            if profile
            else None
        ),
    )


@router.get("/me", response_model=UserOut)
async def me(user: CurrentUser = Depends(get_current_user), session: AsyncSession = Depends(get_session)):
    db_user = await users_repo.get_by_id(session, user.user_id)
    if db_user is None:  # deleted between token check and now
        raise NotFoundError("User not found")
    return to_user_out(db_user, await users_repo.get_recruiter_profile(session, user.user_id))


@router.post("/{user_id}/suspend", status_code=status.HTTP_204_NO_CONTENT)
async def suspend(
    user_id: uuid.UUID,
    current: CurrentUser = Depends(require_role("ADMIN")),
    session: AsyncSession = Depends(get_session),
    redis: Redis = Depends(get_redis),
    settings: Settings = Depends(get_settings),
):
    """Admin: block a user everywhere, immediately (gateway + all services), and end their sessions."""
    await admin.suspend_user(
        session, redis, user_id=user_id, admin_id=current.user_id, ttl_seconds=settings.access_token_ttl_seconds
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
