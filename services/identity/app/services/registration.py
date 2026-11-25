"""Use case: register a new user (Step 2.4; design v1.1: several roles, name on the user; design v2.0/2.1:
the orchestrator's RegistrationWorkflow does the follow-ups).

Two halves:
  register_user  the public request: checks, hashes the password, asks the orchestrator to run "register".
                 The orchestrator's first step calls create_user below; the caller waits for it.
  create_user    the commit step (POST /internal/users): one transaction, safe to run twice."""

import hashlib

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from smarthire_common.models import new_id
from smarthire_common.orchestrator_client import OrchestratorClient

from app.domain.errors import EmailAlreadyRegisteredError
from app.domain.passwords import hash_password
from app.models import User
from app.repositories import users as users_repo


def email_hash(email: str) -> str:
    """The workflow id must not contain the email (ids are visible in Temporal UI, unencrypted)."""
    return hashlib.sha256(email.encode()).hexdigest()


async def register_user(
    session: AsyncSession,
    orchestrator: OrchestratorClient,
    *,
    email: str,
    password: str,
    full_name: str,
    roles: list[str],
    current_company: str | None = None,
) -> dict:
    email = email.strip().lower()
    # Friendly early check, without starting a workflow. The real guard is the unique index in create_user
    if await users_repo.get_by_email(session, email) is not None:
        raise EmailAlreadyRegisteredError()
    user = {
        "id": str(new_id()),  # made HERE, so a retried commit step finds its own earlier insert
        "email": email,
        # Hashed before anything leaves this service: the password never enters Temporal (encrypted or not)
        "password_hash": hash_password(password),
        "full_name": full_name,
        "current_company": current_company,
        "roles": users_repo.in_role_order(roles),
    }
    # Give the DB connection back BEFORE waiting for the orchestrator: its commit step calls OUR
    # /internal/users, which needs a connection from the same pool. Holding ours while waiting deadlocked
    # the pool under load (found in the Step T11 load test: 20 parallel registrations, all 503)
    await session.rollback()
    return await orchestrator.run_action("register", key={"email_hash": email_hash(email)}, input={"user": user})


async def create_user(session: AsyncSession, *, id, email: str, password_hash: str, full_name: str,
                      current_company: str | None, roles: list[str]) -> tuple[User, list[str]]:
    """The commit step. Same id again (a retried step) -> the existing user, nothing written twice."""
    existing = await users_repo.get_by_id(session, id)
    if existing is not None:
        return existing, await users_repo.get_roles(session, id)
    if await users_repo.get_by_email(session, email) is not None:
        raise EmailAlreadyRegisteredError()
    user = User(id=id, email=email, password_hash=password_hash, full_name=full_name, current_company=current_company)
    try:
        await users_repo.add(session, user)
        for role in roles:
            await users_repo.is_role_added(session, user.id, role, granted_by=None)  # self-granted
        # Commit HERE, not in get_session (its commit runs after the response): a duplicate email
        # must become a 409 answer, not a lost commit after "200"
        await session.commit()
    except IntegrityError:
        # The real guard: uq_users_email_lower rejected a concurrent duplicate
        await session.rollback()
        raise EmailAlreadyRegisteredError() from None
    return user, roles
