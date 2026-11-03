"""Internal API: for other SERVICES (Notification needs recipients' email addresses), never for browsers.

The gateway BLOCKS /api/identity/internal/* (infra/apisix/apisix.yaml, route `block-internal`), and the
service port is published on 127.0.0.1 only. No user token on these calls (a Celery task or consumer
has no user), so access is controlled by the network for now.
TODO(Phase 14): service-to-service authentication (service tokens or mTLS) on /internal/*."""

import uuid

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from smarthire_common.db import get_session
from smarthire_common.errors import NotFoundError
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import users as users_repo

router = APIRouter(prefix="/internal", tags=["internal"])


class Contact(BaseModel):
    user_id: uuid.UUID
    email: str
    role: str
    status: str  # ACTIVE / SUSPENDED: callers may choose not to email suspended users


@router.get("/users/{user_id}/contact", response_model=Contact)
async def contact(user_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    user = await users_repo.get_by_id(session, user_id)
    if user is None:
        raise NotFoundError("User not found", code="USER_NOT_FOUND")
    return Contact(user_id=user.id, email=user.email, role=user.role, status=user.status)
