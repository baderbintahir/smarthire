"""The one synchronous call Application makes: Candidate's API, at apply time, for the applicant's
name (the candidate_name snapshot), experience (eligibility) and whether they have a resume.

The user's OWN token is forwarded, so Candidate applies its usual rules (rule 7): we can only ever
read the profile of the person who is applying. If Candidate is down, applying fails with 503;
everything else in Application keeps working.

A CIRCUIT BREAKER (Step 14.2) guards the call: after 5 failures in a row, applies fail at once (503)
for 15 s instead of each waiting up to 3 s for a Candidate that is down. In a hiring spike that
keeps Application's own connections and workers free."""

from decimal import Decimal
from typing import Protocol

import httpx
from smarthire_common.breaker import CircuitBreaker
from smarthire_common.errors import ServiceUnavailableError

from app.domain.applications import CandidateFacts
from app.domain.errors import ProfileIncompleteError


class CandidateClient(Protocol):
    async def facts(self, authorization: str) -> CandidateFacts: ...

    async def close(self) -> None: ...


UNAVAILABLE = "Candidate profiles are unavailable right now; try again shortly"


class HttpCandidateClient:
    def __init__(self, base_url: str, *, timeout: float = 3.0, transport: httpx.AsyncBaseTransport | None = None,
                 breaker: CircuitBreaker | None = None) -> None:
        # Short timeout: a slow Candidate must not hold the applicant's request for long
        self._http = httpx.AsyncClient(base_url=base_url, timeout=timeout, transport=transport)
        self.breaker = breaker or CircuitBreaker(failures=5, reset_seconds=15)

    async def facts(self, authorization: str) -> CandidateFacts:
        if not self.breaker.allow():
            raise ServiceUnavailableError(UNAVAILABLE)  # open: don't even try
        try:
            response = await self._http.get("/candidates/me", headers={"Authorization": authorization})
        except httpx.HTTPError:
            self.breaker.failure()
            raise ServiceUnavailableError(UNAVAILABLE) from None
        if response.status_code >= 500:
            self.breaker.failure()
            raise ServiceUnavailableError(UNAVAILABLE)
        self.breaker.success()  # Candidate answered (even a 4xx is an answer about THIS user, not an outage)
        if response.status_code == 404:
            raise ProfileIncompleteError("Create your candidate profile before applying")
        if response.status_code != 200:
            raise ServiceUnavailableError(UNAVAILABLE)
        profile = response.json()
        years = profile.get("experience_years")
        return CandidateFacts(
            full_name=profile["full_name"],
            experience_years=Decimal(str(years)) if years is not None else None,
            has_resume=profile.get("active_resume") is not None,
        )

    async def close(self) -> None:
        await self._http.aclose()
