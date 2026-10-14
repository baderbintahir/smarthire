"""Admin command line. ADMIN accounts can't self-register (POST /auth/register rejects the role),
so they are created here, by someone with shell access to the service:

    docker compose exec -it identity python -m app.cli create-admin admin@example.com
"""

import argparse
import asyncio
import getpass
import sys

from smarthire_common.db import close_db, init_db, session_scope

from app.config import Settings
from app.services.registration import register_user


async def create_admin(email: str, password: str) -> None:
    init_db(Settings().database_url)
    try:
        async with session_scope() as session:
            user, _ = await register_user(session, email=email, password=password, role="ADMIN")
        print(f"Created ADMIN {user.email} ({user.id})")
    finally:
        await close_db()


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m app.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create-admin")
    create.add_argument("email")
    args = parser.parse_args()

    # Read the password interactively (or from stdin when piped): never as a CLI argument,
    # which would end up in shell history and the process list
    password = getpass.getpass("Password: ") if sys.stdin.isatty() else sys.stdin.readline().strip()
    if len(password) < 8:
        sys.exit("Password must be at least 8 characters")
    asyncio.run(create_admin(args.email, password))


if __name__ == "__main__":
    main()
