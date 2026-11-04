"""Retention jobs for Identity (Step 14.3b, Appendix E.14): outbox, and refresh tokens that expired or
were revoked more than 30 days ago (kept a while for reuse detection and support questions, then useless).

Run them all now (instead of waiting for the schedule): `docker compose exec identity python -m app.maintenance`."""

import asyncio

from smarthire_common.db import close_db, init_db
from smarthire_common.retention import RetentionJob, delete_in_batches, outbox_job, run_all

from app.config import Settings


async def _old_refresh_tokens() -> int:
    return await delete_in_batches(
        "DELETE FROM refresh_tokens WHERE id IN (SELECT id FROM refresh_tokens"
        " WHERE expires_at < now() - interval '30 days' OR revoked_at < now() - interval '30 days' LIMIT :batch)")


JOBS = [outbox_job(), RetentionJob("refresh_tokens", _old_refresh_tokens)]


async def _main() -> None:
    settings = Settings()
    init_db(settings.database_url)
    try:
        print(await run_all(JOBS))
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(_main())
