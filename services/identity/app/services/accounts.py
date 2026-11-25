"""Use cases on a user's own account and roles (design v1.1; design v2.0/2.1: roles change through the
orchestrator's RoleChangeWorkflow, which also creates the candidate profile and records the facts).

Role changes have two halves, like registration:
  grant_role / revoke_role      the public request: rule checks with friendly answers, then the action
  ensure_role / ensure_revoked  the commit steps (internal endpoints): idempotent, safe to run twice
Profile edits are plain writes: no other service reacts to them, so no workflow (and no event)."""

import uuid

from redis.asyncio import Redis
from smarthire_common.errors import NotFoundError
from smarthire_common.orchestrator_client import OrchestratorClient
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
    """changes: only the fields the client sent (PATCH semantics). The token's `name` follows on refresh."""
    user = await _user_or_404(session, user_id)
    changed = {field: value for field, value in changes.items() if getattr(user, field) != value}
    if changed:
        await users_repo.update_profile(session, user_id, changed)
        await session.commit()  # before the response (get_session would commit after it)
        await session.refresh(user)
    return user


# ---------- grant ----------


async def grant_role(session: AsyncSession, orchestrator: OrchestratorClient, user_id: uuid.UUID, role: str, *,
                     granted_by: uuid.UUID | None) -> dict:
    """granted_by None = the user gave it to themselves. The new role shows up in the NEXT token (refresh)."""
    await _user_or_404(session, user_id)
    if role in await users_repo.get_roles(session, user_id):
        raise RoleAlreadyHeldError()
    return await orchestrator.run_action(
        "grant-role", key={"user_id": str(user_id), "role": role},
        input={"user_id": str(user_id), "role": role, "granted_by": str(granted_by) if granted_by else None})


async def ensure_role(session: AsyncSession, user_id: uuid.UUID, role: str, *, granted_by: uuid.UUID | None) -> User:
    """Commit step: the user holds the role afterwards. Already held (a retried step): nothing changes."""
    user = await _user_or_404(session, user_id)
    await users_repo.add_role(session, user_id, role, granted_by)  # ON CONFLICT DO NOTHING
    await session.commit()
    return user


# ---------- revoke ----------


async def revoke_role(session: AsyncSession, orchestrator: OrchestratorClient, user_id: uuid.UUID, role: str, *,
                      revoked_by: uuid.UUID) -> None:
    await _user_or_404(session, user_id)
    roles = await users_repo.get_roles(session, user_id)
    if role not in roles:
        raise RoleNotHeldError()
    if roles == [role]:
        raise LastRoleError()
    await orchestrator.run_action("revoke-role", key={"user_id": str(user_id), "role": role},
                                  input={"user_id": str(user_id), "role": role, "revoked_by": str(revoked_by)})


async def ensure_revoked(session: AsyncSession, redis: Redis, user_id: uuid.UUID, role: str, *, ttl_seconds: int) -> None:
    """Commit step. Tokens carry roles, so a token issued before this still says the user holds the role:
    kill them all (token_version for Identity, revoked_before for the gateway and every service). Not held
    any more (a retried step): nothing to do."""
    await _user_or_404(session, user_id)
    if not await users_repo.remove_role(session, user_id, role):
        return
    if not await users_repo.get_roles(session, user_id):
        await session.rollback()
        raise LastRoleError()  # checked again here: a concurrent revoke may have taken the other role
    await users_repo.bump_token_version(session, user_id)
    # Redis BEFORE commit, as in suspend_user: if the commit then fails, tokens are revoked but the
    # role is kept (safe direction); the other order could leave live tokens claiming a revoked role
    await admin.revoke_all_access_tokens(redis, user_id=user_id, ttl_seconds=ttl_seconds)
    await session.commit()
