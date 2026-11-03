"""Phase 12B against FAKE downstream services (httpx.MockTransport), so each can be made slow,
broken or refusing on purpose. Real Redis (the token denylist) and real token checks."""

import asyncio
import time
import uuid

import httpx
import pytest
from fastapi.testclient import TestClient
from smarthire_common import testing

APP_ID, CANDIDATE_ID, JOB_ID = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())


class FakeServices:
    """Answers like the real services. `down` = hosts that fail (500); `refuse` = host -> status."""

    def __init__(self, down=(), refuse=None, delay=0.0):
        self.down, self.refuse, self.delay = set(down), refuse or {}, delay
        self.calls: list[tuple[str, str, str | None]] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        host, path = request.url.host, request.url.path
        self.calls.append((host, path, request.headers.get("authorization")))
        await asyncio.sleep(self.delay)
        if host in self.down:
            return httpx.Response(500, json={})
        if host in self.refuse:
            return httpx.Response(self.refuse[host], json={"error": {"code": "NOT_ALLOWED", "message": "not yours"}})
        if host == "application" and path.endswith("/summary"):
            return httpx.Response(200, json={"job_id": JOB_ID, "total": 3, "by_stage": {"SCREENING": 2, "Interview": 1}})
        if host == "application":
            return httpx.Response(200, json={"id": APP_ID, "candidate_id": CANDIDATE_ID, "status": "SCREENING"})
        if host == "candidate" and path.endswith("/resume/link"):
            return httpx.Response(200, json={"url": "http://localhost:9000/resumes/x?X-Amz-Signature=s", "expires_in": 300})
        if host == "candidate":
            return httpx.Response(200, json={"id": CANDIDATE_ID, "full_name": "Jane Doe"})
        if host == "scoring":
            return httpx.Response(200, json={"application_id": APP_ID, "score": 82.5, "breakdown": {"missing_skills": []},
                                             "scored_at": "2026-11-03T10:00:00Z", "candidate_id": CANDIDATE_ID})
        if host == "job":
            return httpx.Response(200, json={"items": [{"id": JOB_ID, "title": "Data Engineer", "status": "READY"}],
                                             "next_cursor": None})
        if host == "analytics":
            return httpx.Response(200, json={"total_jobs_published": 7, "total_applications": 40,
                                             "application_to_interview_rate": 0.25, "average_days_to_hire": 6.5,
                                             "most_applied_jobs": []})
        return httpx.Response(404, json={})


@pytest.fixture(scope="session")
def redis_url():
    with testing.redis_container() as url:
        yield url


@pytest.fixture(scope="session")
def tokens():
    return testing.TokenFactory()


def make_client(redis_url, tokens, services: FakeServices, **settings_overrides) -> TestClient:
    from app.config import Settings
    from app.main import create_app

    testing.run(testing.flush_redis(redis_url))
    settings = Settings(redis_url=redis_url, application_url="http://application", candidate_url="http://candidate",
                        job_url="http://job", scoring_url="http://scoring", analytics_url="http://analytics",
                        _env_file=None, **settings_overrides)
    return TestClient(create_app(settings, key_provider=tokens.key_provider, transport=httpx.MockTransport(services)))


# ---------------------------------------------------------------- the application view


def test_composed_view_and_the_token_is_forwarded(redis_url, tokens):
    services = FakeServices()
    headers = tokens.headers(role="RECRUITER")
    with make_client(redis_url, tokens, services) as client:
        body = client.get(f"/bff/applications/{APP_ID}", headers=headers).json()
    assert body["application"]["status"] == "SCREENING" and body["candidate"]["full_name"] == "Jane Doe"
    assert body["score"] == {"score": 82.5, "breakdown": {"missing_skills": []}, "scored_at": "2026-11-03T10:00:00Z"}
    assert body["resume_download"]["expires_in"] == 300 and body["degraded"] == []
    forwarded = {host: auth for host, _, auth in services.calls}
    assert forwarded["application"] == headers["Authorization"] == forwarded["candidate"]  # the USER's token
    assert forwarded["scoring"] is None  # internal API, called only after Application allowed the caller


def test_scoring_down_degrades_instead_of_failing(redis_url, tokens):
    with make_client(redis_url, tokens, FakeServices(down={"scoring"})) as client:
        r = client.get(f"/bff/applications/{APP_ID}", headers=tokens.headers(role="RECRUITER"))
    assert r.status_code == 200
    assert r.json()["score"] is None and r.json()["degraded"] == ["score"]
    assert r.json()["candidate"]["full_name"] == "Jane Doe"  # the rest of the page is there


def test_application_refusal_is_passed_through(redis_url, tokens):
    services = FakeServices(refuse={"application": 403})
    with make_client(redis_url, tokens, services) as client:
        r = client.get(f"/bff/applications/{APP_ID}", headers=tokens.headers(role="RECRUITER"))
    assert r.status_code == 403 and r.json()["error"]["code"] == "NOT_ALLOWED"
    assert [host for host, _, _ in services.calls] == ["application"]  # nothing else was fetched


def test_application_down_is_503(redis_url, tokens):
    with make_client(redis_url, tokens, FakeServices(down={"application"})) as client:
        assert client.get(f"/bff/applications/{APP_ID}", headers=tokens.headers(role="CANDIDATE")).status_code == 503


def test_optional_calls_run_in_parallel(redis_url, tokens):
    with make_client(redis_url, tokens, FakeServices(delay=0.3)) as client:
        started = time.perf_counter()
        client.get(f"/bff/applications/{APP_ID}", headers=tokens.headers(role="RECRUITER"))
        elapsed = time.perf_counter() - started
    assert elapsed < 0.9  # application (0.3) + 3 parallel calls (0.3), not 4 x 0.3 = 1.2


def test_slow_service_times_out_and_degrades(redis_url, tokens):
    class SlowScoring(FakeServices):
        async def __call__(self, request):
            if request.url.host == "scoring":
                await asyncio.sleep(1.0)
            return await super().__call__(request)

    with make_client(redis_url, tokens, SlowScoring(), call_timeout_seconds=0.2) as client:
        body = client.get(f"/bff/applications/{APP_ID}", headers=tokens.headers(role="RECRUITER")).json()
    assert body["degraded"] == ["score"]


def test_breaker_stops_calling_a_failing_service(redis_url, tokens):
    services = FakeServices(down={"scoring"})
    with make_client(redis_url, tokens, services, breaker_failures=2) as client:
        for _ in range(4):
            client.get(f"/bff/applications/{APP_ID}", headers=tokens.headers(role="RECRUITER"))
    assert sum(1 for host, _, _ in services.calls if host == "scoring") == 2  # then the breaker was open


def test_token_is_required(redis_url, tokens):
    with make_client(redis_url, tokens, FakeServices()) as client:
        assert client.get(f"/bff/applications/{APP_ID}").status_code == 401

# ---------------------------------------------------------------- the recruiter dashboard


def test_dashboard(redis_url, tokens):
    with make_client(redis_url, tokens, FakeServices()) as client:
        body = client.get("/bff/recruiter/dashboard", headers=tokens.headers(role="RECRUITER")).json()
    assert body["jobs"] == [{"id": JOB_ID, "title": "Data Engineer", "status": "READY",
                             "applications": {"SCREENING": 2, "Interview": 1}, "total_applications": 3}]
    assert body["metrics"] == {"total_jobs_published": 7, "total_applications": 40,
                               "application_to_interview_rate": 0.25, "average_days_to_hire": 6.5}
    assert body["degraded"] == []


def test_dashboard_degrades_per_part(redis_url, tokens):
    with make_client(redis_url, tokens, FakeServices(down={"analytics", "application"})) as client:
        body = client.get("/bff/recruiter/dashboard", headers=tokens.headers(role="RECRUITER")).json()
    assert body["metrics"] is None and body["jobs"][0]["applications"] is None
    assert body["degraded"] == ["application_counts", "metrics"] and body["jobs"][0]["title"] == "Data Engineer"


def test_dashboard_is_for_recruiters(redis_url, tokens):
    with make_client(redis_url, tokens, FakeServices()) as client:
        assert client.get("/bff/recruiter/dashboard", headers=tokens.headers(role="CANDIDATE")).status_code == 403
