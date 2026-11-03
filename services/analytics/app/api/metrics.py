"""Metrics API (Step 12.3). Platform-wide numbers: for recruiters and admins, not candidates."""

from fastapi import APIRouter, Depends, Request
from smarthire_common.auth import CurrentUser, require_role
from smarthire_common.db import get_session
from smarthire_common.redis import get_redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import metrics

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/metrics")
async def get_metrics(request: Request, user: CurrentUser = Depends(require_role("RECRUITER", "ADMIN")),
                      session: AsyncSession = Depends(get_session)) -> dict:
    return await metrics.cached(session, get_redis(), request.app.state.settings.metrics_cache_seconds)
