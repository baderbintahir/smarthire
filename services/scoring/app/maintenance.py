"""Retention jobs for Scoring (Step 14.3b, Appendix E.14): published documents in the Mongo outbox.

Run them all now (instead of waiting for the schedule): `docker compose exec scoring python -m app.maintenance`."""

import asyncio

from smarthire_common.mongo import close_mongo, init_mongo
from smarthire_common.retention import mongo_outbox_job, run_all

from app.config import Settings

JOBS = [mongo_outbox_job()]


async def _main() -> None:
    settings = Settings()
    init_mongo(settings.mongo_url, settings.mongo_database)
    try:
        print(await run_all(JOBS))
    finally:
        await close_mongo()


if __name__ == "__main__":
    asyncio.run(_main())
