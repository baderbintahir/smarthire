import uuid

from fastapi import APIRouter, Depends, Response, status
from redis.asyncio import Redis
from smarthire_common.db import get_session
from smarthire_common.redis import get_redis
from smarthire_common.errors import NotFoundError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_current_user, get_orchestrator, get_settings, require_role
from app.config import Settings
from app.models import User
from app.repositories import users as users_repo
from app.schemas.auth import AdminRoleRequest, AnyRole, ProfileUpdate, SelfRoleRequest, UserOut
from app.services import accounts, admin

router = APIRouter(prefix="/users", tags=["users"])


def to_user_out(user: User, roles: list[str]) -> UserOut:
    return UserOut(
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        current_company=user.current_company,
        roles=roles,
        status=user.status,
        created_at=user.created_at,
    )


async def _user_out(session: AsyncSession, user: User) -> UserOut:
    return to_user_out(user, await users_repo.get_roles(session, user.id))


@router.get("/me", response_model=UserOut)
async def me(user: CurrentUser = Depends(get_current_user), session: AsyncSession = Depends(get_session)):
    db_user = await users_repo.get_by_id(session, user.user_id)
    if db_user is None:  # deleted between token check and now
        raise NotFoundError("User not found")
    return await _user_out(session, db_user)


@router.patch("/me", response_model=UserOut)
async def update_me(
    body: ProfileUpdate, user: CurrentUser = Depends(get_current_user), session: AsyncSession = Depends(get_session)
):
    """Change my name and/or current employer (a plain write: no other service reacts). The token's
    `name` claim updates on the next refresh."""
    changes = body.model_dump(include=body.model_fields_set)  # only what the client sent
    return await _user_out(session, await accounts.update_profile(session, user.user_id, changes))


@router.post("/me/roles", status_code=status.HTTP_201_CREATED, response_model=UserOut)
async def add_my_role(
    body: SelfRoleRequest, user: CurrentUser = Depends(get_current_user), session: AsyncSession = Depends(get_session),
    orchestrator=Depends(get_orchestrator),
):
    """Give myself CANDIDATE or RECRUITER (never ADMIN). 409 if I already hold it. Refresh to get
    a token that carries the new role. Design v2.1: RoleChangeWorkflow (profile + fact follow)."""
    return await accounts.grant_role(session, orchestrator, user.user_id, body.role, granted_by=None)


@router.post("/{user_id}/roles", status_code=status.HTTP_201_CREATED, response_model=UserOut)
async def grant_role(
    user_id: uuid.UUID,
    body: AdminRoleRequest,
    current: CurrentUser = Depends(require_role("ADMIN")),
    session: AsyncSession = Depends(get_session),
    orchestrator=Depends(get_orchestrator),
):
    """Admin: grant any role, ADMIN included."""
    return await accounts.grant_role(session, orchestrator, user_id, body.role, granted_by=current.user_id)


@router.delete("/{user_id}/roles/{role}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_role(
    user_id: uuid.UUID,
    role: AnyRole,
    current: CurrentUser = Depends(require_role("ADMIN")),
    session: AsyncSession = Depends(get_session),
    orchestrator=Depends(get_orchestrator),
):
    """Admin: take a role away. The user's current tokens stop working everywhere (they still list the role)."""
    await accounts.revoke_role(session, orchestrator, user_id, role, revoked_by=current.user_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


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
