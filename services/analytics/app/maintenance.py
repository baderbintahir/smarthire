"""Retention jobs for Analytics (Step 14.3b, Appendix E.14): processed_events.

A rebuild (app.cli) empties processed_events anyway; between rebuilds the table would grow by one row per
event ever consumed.

Run them all now (instead of waiting for the schedule): `docker compose exec analytics python -m app.maintenance`."""

import asyncio

from smarthire_common.db import close_db, init_db
from smarthire_common.retention import processed_events_job, run_all

from app.config import Settings

JOBS = [processed_events_job()]


async def _main() -> None:
    settings = Settings()
    init_db(settings.database_url)
    try:
        print(await run_all(JOBS))
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(_main())
