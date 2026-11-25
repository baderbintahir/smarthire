"""Admin commands.

    docker compose exec analytics python -m app.cli rebuild
        Rebuild the read model from Analytics' own `facts` table (design v2.0): empty the projections and
        re-apply every fact in time order, in one transaction. Analytics can keep running meanwhile: new
        facts wait for the rebuild to commit. The numbers come back identical, the proof that the
        projections are only a VIEW of the facts.
"""

import argparse
import asyncio

from smarthire_common.db import close_db, init_db, session_scope
from smarthire_common.logging import setup_logging
from smarthire_common.redis import close_redis, get_redis, init_redis

from app.config import Settings
from app.services import facts
from app.services.metrics import CACHE_KEY


async def rebuild(settings: Settings) -> int:
    init_db(settings.database_url)
    init_redis(settings.redis_url)
    try:
        async with session_scope() as session:
            count = await facts.rebuild(session)
        await get_redis().delete(CACHE_KEY)  # don't serve numbers cached before the rebuild
        return count
    finally:
        await close_redis()
        await close_db()


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m app.cli")
    parser.add_argument("command", choices=["rebuild"])
    parser.parse_args()
    settings = Settings()
    setup_logging(f"{settings.service_name}-cli", settings.log_level)
    print(f"rebuilt the analytics read model from {asyncio.run(rebuild(settings))} facts")


if __name__ == "__main__":
    main()
