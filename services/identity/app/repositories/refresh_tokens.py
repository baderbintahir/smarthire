"""All SQL for refresh tokens."""

import uuid
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import RefreshToken


async def add(session: AsyncSession, token: RefreshToken) -> None:
    session.add(token)
    await session.flush()


async def get_by_hash(session: AsyncSession, token_hash: bytes, *, for_update: bool = False) -> RefreshToken | None:
    stmt = select(RefreshToken).where(RefreshToken.token_hash == token_hash)
    if for_update:
        # Row lock until commit: if two requests refresh with the SAME token at the same time,
        # the second waits, then sees used_at already set and is treated as reuse.
        stmt = stmt.with_for_update()
    return (await session.execute(stmt)).scalar_one_or_none()


async def revoke_family(session: AsyncSession, family_id: uuid.UUID, now: datetime) -> None:
    await session.execute(
        update(RefreshToken)
        .where(RefreshToken.family_id == family_id, RefreshToken.revoked_at.is_(None))
        .values(revoked_at=now)
    )


async def revoke_all_for_user(session: AsyncSession, user_id: uuid.UUID, now: datetime) -> None:
    await session.execute(
        update(RefreshToken)
        .where(RefreshToken.user_id == user_id, RefreshToken.revoked_at.is_(None))
        .values(revoked_at=now)
    )
