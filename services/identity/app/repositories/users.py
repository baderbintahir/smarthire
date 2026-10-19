"""All SQL for users and recruiter profiles. Services call these; routers never do."""

import uuid

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import RecruiterProfile, User


async def get_by_email(session: AsyncSession, email: str) -> User | None:
    # lower(email) matches the expression in uq_users_email_lower, so this is one index lookup
    stmt = select(User).where(func.lower(User.email) == email.lower())
    return (await session.execute(stmt)).scalar_one_or_none()


async def get_by_id(session: AsyncSession, user_id: uuid.UUID) -> User | None:
    return await session.get(User, user_id)


async def get_recruiter_profile(session: AsyncSession, user_id: uuid.UUID) -> RecruiterProfile | None:
    return await session.get(RecruiterProfile, user_id)


async def add(session: AsyncSession, user: User, profile: RecruiterProfile | None = None) -> None:
    session.add(user)
    # Flush the user first: the profile's foreign key needs the users row to exist
    await session.flush()
    if profile is not None:
        session.add(profile)
        await session.flush()


async def set_password_hash(session: AsyncSession, user: User, password_hash: str) -> None:
    user.password_hash = password_hash
    user.updated_at = func.now()  # E.2: updated_at is set by the repository on every update


async def bump_token_version(session: AsyncSession, user_id: uuid.UUID) -> None:
    # Done in SQL (token_version + 1), not read-modify-write in Python: two concurrent
    # "logout everywhere" calls can't overwrite each other's increment
    await session.execute(
        update(User)
        .where(User.id == user_id)
        .values(token_version=User.token_version + 1, updated_at=func.now())
    )


async def set_status(session: AsyncSession, user_id: uuid.UUID, status: str) -> None:
    await session.execute(
        update(User).where(User.id == user_id).values(status=status, updated_at=func.now())
    )
