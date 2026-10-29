"""Use case: register a new user (Step 2.4)."""

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from smarthire_common.events import new_event
from smarthire_common.models import new_id
from smarthire_common.outbox import add_outbox_event

from app.domain.errors import EmailAlreadyRegisteredError
from app.domain.passwords import hash_password
from app.models import RecruiterProfile, User
from app.repositories import users as users_repo


async def register_user(
    session: AsyncSession,
    *,
    email: str,
    password: str,
    role: str,
    display_name: str | None = None,
    company: str | None = None,
) -> tuple[User, RecruiterProfile | None]:
    email = email.strip().lower()

    # Friendly early check. Not enough on its own: two requests can both pass it at once.
    if await users_repo.get_by_email(session, email) is not None:
        raise EmailAlreadyRegisteredError()

    # id set explicitly (not left to the column default, which only fires at INSERT) because
    # the profile needs it right now
    user = User(id=new_id(), email=email, password_hash=hash_password(password), role=role)
    profile = None
    if role == "RECRUITER":
        profile = RecruiterProfile(user_id=user.id, display_name=display_name, company=company)
    try:
        await users_repo.add(session, user, profile)
        # Tell other services, in the SAME transaction: the event exists if and only if the user
        # does (Candidate creates a profile from it). IDs only, no email or name (E.1 principle 10)
        event = new_event("UserRegistered", producer="identity", payload={"user_id": str(user.id), "role": role})
        await add_outbox_event(session, event, key=str(user.id))
        # Commit HERE, not in get_session: its commit runs after the response is sent, so a
        # failure there could not become a 409. User + profile + event commit together or not at all.
        await session.commit()
    except IntegrityError:
        # The real guard: uq_users_email_lower rejected a concurrent duplicate
        await session.rollback()
        raise EmailAlreadyRegisteredError() from None
    return user, profile
