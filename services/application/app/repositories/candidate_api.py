"""The one synchronous call Application makes: Candidate's API, at apply time, for the applicant's
name (the candidate_name snapshot), experience (eligibility) and whether they have a resume.

The user's OWN token is forwarded, so Candidate applies its usual rules (rule 7): we can only ever
read the profile of the person who is applying. If Candidate is down, applying fails with 503;
everything else in Application keeps working."""

from decimal import Decimal
from typing import Protocol

import httpx
from smarthire_common.errors import ServiceUnavailableError

from app.domain.applications import CandidateFacts
from app.domain.errors import ProfileIncompleteError


class CandidateClient(Protocol):
    async def facts(self, authorization: str) -> CandidateFacts: ...

    async def close(self) -> None: ...


class HttpCandidateClient:
    def __init__(self, base_url: str, *, timeout: float = 3.0) -> None:
        # Short timeout: a slow Candidate must not hold the applicant's request for long
        self._http = httpx.AsyncClient(base_url=base_url, timeout=timeout)

    async def facts(self, authorization: str) -> CandidateFacts:
        try:
            response = await self._http.get("/candidates/me", headers={"Authorization": authorization})
        except httpx.HTTPError:
            raise ServiceUnavailableError("Candidate profiles are unavailable right now; try again shortly") from None
        if response.status_code == 404:
            raise ProfileIncompleteError("Create your candidate profile before applying")
        if response.status_code != 200:
            raise ServiceUnavailableError("Candidate profiles are unavailable right now; try again shortly")
        profile = response.json()
        years = profile.get("experience_years")
        return CandidateFacts(
            full_name=profile["full_name"],
            experience_years=Decimal(str(years)) if years is not None else None,
            has_resume=profile.get("active_resume") is not None,
        )

    async def close(self) -> None:
        await self._http.aclose()
