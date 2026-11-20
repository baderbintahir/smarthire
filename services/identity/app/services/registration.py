"""Use case: register a new user (Step 2.4; design v1.1: several roles, name on the user)."""

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from smarthire_common.events import new_event
from smarthire_common.models import new_id
from smarthire_common.outbox import add_outbox_event

from app.domain.errors import EmailAlreadyRegisteredError
from app.domain.passwords import hash_password
from app.models import User
from app.repositories import users as users_repo


async def register_user(
    session: AsyncSession,
    *,
    email: str,
    password: str,
    full_name: str,
    roles: list[str],
    current_company: str | None = None,
) -> tuple[User, list[str]]:
    email = email.strip().lower()

    # Friendly early check. Not enough on its own: two requests can both pass it at once.
    if await users_repo.get_by_email(session, email) is not None:
        raise EmailAlreadyRegisteredError()

    user = User(id=new_id(), email=email, password_hash=hash_password(password),
                full_name=full_name, current_company=current_company)
    try:
        await users_repo.add(session, user)
        for role in roles:
            await users_repo.add_role(session, user.id, role, granted_by=None)  # self-granted
        # Tell other services, in the SAME transaction: the event exists if and only if the user
        # does (Candidate creates a profile from it). IDs and roles only, no email or name
        # (E.1 principle 10). `role` is the deprecated v1 field, kept for BACKWARD compatibility.
        payload = {"user_id": str(user.id), "role": roles[0], "roles": roles}
        await add_outbox_event(session, new_event("UserRegistered", producer="identity", payload=payload), key=str(user.id))
        # Commit HERE, not in get_session: its commit runs after the response is sent, so a
        # failure there could not become a 409. User + roles + event commit together or not at all.
        await session.commit()
    except IntegrityError:
        # The real guard: uq_users_email_lower rejected a concurrent duplicate
        await session.rollback()
        raise EmailAlreadyRegisteredError() from None
    return user, roles
