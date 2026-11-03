"""HTTP layer of the BFF (reached as /api/bff/* through the gateway)."""

import uuid

from fastapi import APIRouter, Depends, Request
from smarthire_common.auth import CurrentUser, get_current_user, require_role

from app.services import screens

router = APIRouter(prefix="/bff", tags=["bff"])


@router.get("/applications/{application_id}")
async def application_view(application_id: uuid.UUID, request: Request,
                           user: CurrentUser = Depends(get_current_user)) -> dict:
    """The application page: the application + the candidate's profile + the score + a resume link.
    Who may see it is decided by Application (the BFF forwards the caller's own token)."""
    return await screens.application_view(request.app.state.services, str(application_id),
                                          request.headers["authorization"])


@router.get("/recruiter/dashboard")
async def recruiter_dashboard(request: Request, user: CurrentUser = Depends(require_role("RECRUITER"))) -> dict:
    """The recruiter's jobs + applications per stage + headline platform metrics."""
    return await screens.recruiter_dashboard(request.app.state.services, request.headers["authorization"])
