"""Retention jobs for the Application service (Step 14.3b, Appendix E.14): outbox and processed_events.

Run them all now (instead of waiting for the schedule): `docker compose exec application python -m app.maintenance`."""

import asyncio

from smarthire_common.db import close_db, init_db
from smarthire_common.retention import outbox_job, processed_events_job, run_all

from app.config import Settings

JOBS = [outbox_job(), processed_events_job()]


async def _main() -> None:
    settings = Settings()
    init_db(settings.database_url)
    try:
        print(await run_all(JOBS))
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(_main())
