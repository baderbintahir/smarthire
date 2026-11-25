"""Internal API: for other SERVICES, never for browsers. The gateway BLOCKS /api/identity/internal/*
(infra/apisix/apisix.yaml, route `block-internal`), and the service port is published on 127.0.0.1 only.

    GET    /internal/users/{id}/contact      read-only (SendEmailWorkflow's get_contact)
    POST   /internal/users                   create_user: RegistrationWorkflow's commit step
    POST   /internal/users/{id}/roles        grant_role: RoleChangeWorkflow's commit step
    DELETE /internal/users/{id}/roles/{role} revoke_role: RoleChangeWorkflow's commit step

The writing endpoints require the orchestrator's token and an Idempotency-Key (design v2.1), and are
safe to run twice: Temporal may repeat a step after a timeout."""

import uuid

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from smarthire_common.db import get_session
from smarthire_common.errors import NotFoundError
from smarthire_common.internal import idempotency_key
from smarthire_common.redis import get_redis
from smarthire_common.service_auth import require_orchestrator
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_settings
from app.api.users import to_user_out
from app.config import Settings
from app.repositories import users as users_repo
from app.schemas.auth import AnyRole, UserOut
from app.services import accounts, registration

router = APIRouter(prefix="/internal", tags=["internal"])
COMMIT_STEP = [Depends(require_orchestrator), Depends(idempotency_key)]


class Contact(BaseModel):
    user_id: uuid.UUID
    email: str
    roles: list[str]
    status: str  # ACTIVE / SUSPENDED: callers may choose not to email suspended users


@router.get("/users/{user_id}/contact", response_model=Contact)
async def contact(user_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    user = await users_repo.get_by_id(session, user_id)
    if user is None:
        raise NotFoundError("User not found", code="USER_NOT_FOUND")
    roles = await users_repo.get_roles(session, user_id)
    return Contact(user_id=user.id, email=user.email, roles=roles, status=user.status)


class NewUser(BaseModel):
    id: uuid.UUID
    email: str
    password_hash: str  # hashed by the public endpoint; the plain password never leaves it
    full_name: str
    current_company: str | None = None
    roles: list[AnyRole] = Field(min_length=1)


@router.post("/users", response_model=UserOut, dependencies=COMMIT_STEP)
async def create_user(body: NewUser, session: AsyncSession = Depends(get_session)):
    user, roles = await registration.create_user(session, **body.model_dump())
    return to_user_out(user, sorted(roles, key=users_repo.ROLE_IDS.get))


class RoleGrant(BaseModel):
    role: AnyRole
    granted_by: uuid.UUID | None = None


@router.post("/users/{user_id}/roles", response_model=UserOut, dependencies=COMMIT_STEP)
async def grant_role(user_id: uuid.UUID, body: RoleGrant, session: AsyncSession = Depends(get_session)):
    user = await accounts.ensure_role(session, user_id, body.role, granted_by=body.granted_by)
    return to_user_out(user, await users_repo.get_roles(session, user_id))


@router.delete("/users/{user_id}/roles/{role}", status_code=status.HTTP_204_NO_CONTENT, dependencies=COMMIT_STEP)
async def revoke_role(user_id: uuid.UUID, role: AnyRole, session: AsyncSession = Depends(get_session),
                      redis: Redis = Depends(get_redis), settings: Settings = Depends(get_settings)):
    await accounts.ensure_revoked(session, redis, user_id, role, ttl_seconds=settings.access_token_ttl_seconds)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
