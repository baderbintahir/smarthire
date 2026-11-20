"""Use cases on a user's own account and roles (design v1.1, Phase 14R step R3).

Every change other services care about writes its event in the SAME transaction (rule 3):
RoleGranted, RoleRevoked, UserProfileUpdated. Events carry ids and field names, never the name
itself (E.1 principle 10)."""

import uuid

from redis.asyncio import Redis
from smarthire_common.errors import NotFoundError
from smarthire_common.events import new_event
from smarthire_common.outbox import add_outbox_event
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.errors import LastRoleError, RoleAlreadyHeldError, RoleNotHeldError
from app.models import User
from app.repositories import users as users_repo
from app.services import admin


async def _user_or_404(session: AsyncSession, user_id: uuid.UUID) -> User:
    user = await users_repo.get_by_id(session, user_id)
    if user is None:
        raise NotFoundError("User not found", code="USER_NOT_FOUND")
    return user


async def update_profile(session: AsyncSession, user_id: uuid.UUID, changes: dict) -> User:
    """changes: only the fields the client sent (PATCH semantics)."""
    user = await _user_or_404(session, user_id)
    changed = sorted(field for field, value in changes.items() if getattr(user, field) != value)
    if changed:  # a PATCH that changes nothing emits nothing
        await users_repo.update_profile(session, user_id, {field: changes[field] for field in changed})
        event = new_event("UserProfileUpdated", producer="identity", payload={"user_id": str(user_id), "changed": changed})
        await add_outbox_event(session, event, key=str(user_id))
        await session.commit()  # before the response (get_session would commit after it)
        await session.refresh(user)
    return user


async def grant_role(session: AsyncSession, user_id: uuid.UUID, role: str, *, granted_by: uuid.UUID | None) -> User:
    """granted_by None = the user gave it to themselves. The new role shows up in the NEXT token
    (refresh), so nothing is revoked: the current token simply lacks the extra role for a while."""
    user = await _user_or_404(session, user_id)
    if not await users_repo.add_role(session, user_id, role, granted_by):
        raise RoleAlreadyHeldError()
    payload = {"user_id": str(user_id), "role": role, "granted_by": str(granted_by) if granted_by else None}
    await add_outbox_event(session, new_event("RoleGranted", producer="identity", payload=payload), key=str(user_id))
    await session.commit()
    return user


async def revoke_role(
    session: AsyncSession, redis: Redis, user_id: uuid.UUID, role: str, *, revoked_by: uuid.UUID, ttl_seconds: int
) -> None:
    """Tokens carry roles, so a token issued before this still says the user holds the role: kill
    them all (token_version for Identity, revoked_before for the gateway and every service). The
    user logs in again or refreshes and gets a token without the role."""
    await _user_or_404(session, user_id)
    if not await users_repo.remove_role(session, user_id, role):
        raise RoleNotHeldError()
    if not await users_repo.get_roles(session, user_id):
        raise LastRoleError()  # raising before commit: get_session rolls the delete back
    await users_repo.bump_token_version(session, user_id)
    payload = {"user_id": str(user_id), "role": role, "revoked_by": str(revoked_by)}
    await add_outbox_event(session, new_event("RoleRevoked", producer="identity", payload=payload), key=str(user_id))
    # Redis BEFORE commit, as in suspend_user: if the commit then fails, tokens are revoked but the
    # role is kept (safe direction); the other order could leave live tokens claiming a revoked role
    await admin.revoke_all_access_tokens(redis, user_id=user_id, ttl_seconds=ttl_seconds)
    await session.commit()
