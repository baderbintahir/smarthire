import asyncio
import logging
from collections.abc import Awaitable, Callable

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text

from smarthire_common.db import session_scope

logger = logging.getLogger(__name__)

CHECK_TIMEOUT_SECONDS = 2.0

# name -> async function that returns normally if healthy and raises if not
_checks: dict[str, Callable[[], Awaitable[None]]] = {}


def register_readiness_check(name: str, check: Callable[[], Awaitable[None]]) -> None:
    _checks[name] = check


def clear_readiness_checks() -> None:
    _checks.clear()


async def db_ping_check() -> None:
    async with session_scope() as session:
        await session.execute(text("SELECT 1"))


async def _run(name: str, check: Callable[[], Awaitable[None]]) -> tuple[str, bool]:
    try:
        await asyncio.wait_for(check(), timeout=CHECK_TIMEOUT_SECONDS)
        return name, True
    except Exception:
        # Details go to the logs only; the response just says which check failed
        logger.exception("Readiness check failed: %s", name)
        return name, False


health_router = APIRouter(prefix="/health", tags=["health"])


@health_router.get("/live")
async def live() -> dict:
    # Liveness checks nothing external: "the process is up". A failing DB must not restart us.
    return {"status": "ok"}


@health_router.get("/ready")
async def ready():
    # Readiness: "I can serve traffic now". Checks run concurrently, each with a timeout.
    results = dict(await asyncio.gather(*(_run(n, c) for n, c in _checks.items())))
    ok = all(results.values())
    body = {
        "status": "ok" if ok else "unavailable",
        "checks": {n: "ok" if r else "fail" for n, r in results.items()},
    }
    return JSONResponse(body, status_code=200 if ok else 503)
