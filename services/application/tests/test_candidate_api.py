"""Step 14.2: the breaker on Application's one synchronous call (Candidate, at apply time)."""

import httpx
import pytest
from smarthire_common.breaker import CircuitBreaker
from smarthire_common.errors import ServiceUnavailableError

from app.domain.errors import ProfileIncompleteError
from app.repositories.candidate_api import HttpCandidateClient


def client(status: int, calls: list) -> HttpCandidateClient:
    def answer(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(status, json={"full_name": "Jane", "experience_years": 3, "active_resume": {"id": "r"}})

    return HttpCandidateClient("http://candidate", transport=httpx.MockTransport(answer),
                               breaker=CircuitBreaker(failures=2, reset_seconds=60))


async def test_after_repeated_failures_the_breaker_fails_fast_without_calling():
    calls: list = []
    candidates = client(503, calls)
    for _ in range(4):
        with pytest.raises(ServiceUnavailableError):
            await candidates.facts("Bearer t")
    assert len(calls) == 2  # the last two applies failed at once: Candidate wasn't called


async def test_a_4xx_is_an_answer_not_an_outage():
    calls: list = []
    candidates = client(404, calls)
    for _ in range(3):
        with pytest.raises(ProfileIncompleteError):
            await candidates.facts("Bearer t")
    assert len(calls) == 3 and candidates.breaker.opened_at is None


async def test_success():
    facts = await client(200, []).facts("Bearer t")
    assert facts.full_name == "Jane" and facts.has_resume
