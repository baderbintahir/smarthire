"""Retention jobs for Notification (Step 14.3b, Appendix E.14): SENT notification_log rows older than 90 days
(weekly). QUEUED and FAILED rows are kept: a workflow is still retrying them, or a human should look.

Run them all now (instead of waiting for the schedule): `docker compose exec notification python -m app.maintenance`."""

import asyncio

from smarthire_common.db import close_db, init_db
from smarthire_common.retention import DAY, RetentionJob, delete_in_batches, run_all

from app.config import Settings


async def _old_sent_emails() -> int:
    return await delete_in_batches(
        "DELETE FROM notification_log WHERE id IN (SELECT id FROM notification_log"
        " WHERE status = 'SENT' AND sent_at < now() - interval '90 days' LIMIT :batch)")


JOBS = [RetentionJob("notification_log", _old_sent_emails, every_seconds=7 * DAY)]


async def _main() -> None:
    settings = Settings()
    init_db(settings.database_url)
    try:
        print(await run_all(JOBS))
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(_main())
