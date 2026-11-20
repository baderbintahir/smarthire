"""All SQL for users and their roles. Services call these; routers never do."""

import uuid

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import User, UserRole

# Fixed ids seeded by migration 0005 (roles table)
ROLE_IDS = {"CANDIDATE": 1, "RECRUITER": 2, "ADMIN": 3}


async def get_by_email(session: AsyncSession, email: str) -> User | None:
    # lower(email) matches the expression in uq_users_email_lower, so this is one index lookup
    stmt = select(User).where(func.lower(User.email) == email.lower())
    return (await session.execute(stmt)).scalar_one_or_none()


async def get_by_id(session: AsyncSession, user_id: uuid.UUID) -> User | None:
    return await session.get(User, user_id)


async def add(session: AsyncSession, user: User) -> None:
    session.add(user)
    await session.flush()  # the users row must exist before user_roles rows reference it


ROLE_NAMES = {role_id: name for name, role_id in ROLE_IDS.items()}


async def get_roles(session: AsyncSession, user_id: uuid.UUID) -> list[str]:
    """The user's roles in a stable order (role id): the same user always gets the same token claim."""
    stmt = select(UserRole.role_id).where(UserRole.user_id == user_id).order_by(UserRole.role_id)
    return [ROLE_NAMES[role_id] for role_id in (await session.execute(stmt)).scalars()]


async def add_role(session: AsyncSession, user_id: uuid.UUID, role: str, granted_by: uuid.UUID | None) -> bool:
    """Grants a role. False if the user already holds it. ON CONFLICT on the (user_id, role_id)
    primary key: two concurrent grants can't both "succeed" or raise mid-transaction."""
    stmt = (
        pg_insert(UserRole)
        .values(user_id=user_id, role_id=ROLE_IDS[role], granted_by=granted_by)
        .on_conflict_do_nothing(index_elements=["user_id", "role_id"])
        .returning(UserRole.user_id)
    )
    return (await session.execute(stmt)).first() is not None


async def remove_role(session: AsyncSession, user_id: uuid.UUID, role: str) -> bool:
    """False if the user didn't hold the role."""
    stmt = delete(UserRole).where(UserRole.user_id == user_id, UserRole.role_id == ROLE_IDS[role]).returning(UserRole.user_id)
    return (await session.execute(stmt)).first() is not None


async def update_profile(session: AsyncSession, user_id: uuid.UUID, changes: dict) -> None:
    await session.execute(update(User).where(User.id == user_id).values(**changes, updated_at=func.now()))


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
