"""Retention jobs for the Candidate service (Step 14.3b, Appendix E.14): none since design v2.0 (no outbox,
no processed_events). Kept so every service has the same entry point.

Run them all now (instead of waiting for the schedule): `docker compose exec candidate python -m app.maintenance`."""

import asyncio

from smarthire_common.db import close_db, init_db
from smarthire_common.retention import run_all

from app.config import Settings

JOBS: list = []


async def _main() -> None:
    settings = Settings()
    init_db(settings.database_url)
    try:
        print(await run_all(JOBS))
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(_main())
