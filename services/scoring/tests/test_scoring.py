"""Step 10.3: the score formula (pure), then the score_application step (design v2.1: an internal
endpoint the orchestrator calls) on a real MongoDB with the Candidate and Enrichment APIs replaced by
httpx.MockTransport."""

import httpx
import pytest
from pymongo import AsyncMongoClient
from smarthire_common import mongo
from smarthire_common.errors import AppError, ServiceUnavailableError

from app.domain.scoring import score
from app.repositories import peers, scores
from app.services import scoring

# ---------------------------------------------------------------- the formula


def test_perfect_match_is_100():
    result = score(job_skills=["python", "sql", "docker"], required_skills=["python", "sql"],
                   candidate_skills=["Python", "SQL", "Docker"], min_experience_years=3, candidate_years=5)
    assert result["score"] == 100.0 and result["breakdown"]["missing_skills"] == []


def test_points_add_up_and_are_explained():
    result = score(job_skills=["python", "sql", "docker", "kafka"], required_skills=["python", "sql"],
                   candidate_skills=["python", "kafka"], min_experience_years=4, candidate_years=2)
    # required 1/2 of 70 = 35, others 1/2 of 10 = 5, experience 2/4 of 20 = 10
    assert result["score"] == 50.0
    assert result["breakdown"]["matched_skills"] == ["python", "kafka"]
    assert result["breakdown"]["missing_skills"] == ["sql"]
    assert result["breakdown"]["experience_match"] == {"required_years": 4, "candidate_years": 2, "points": 10.0}


def test_aliases_match():
    """Resume says "k8s", the job says "Kubernetes": the shared dictionary makes them equal."""
    result = score(job_skills=["kubernetes"], required_skills=["Kubernetes"], candidate_skills=["k8s"],
                   min_experience_years=None, candidate_years=1)
    assert result["score"] == 100.0


def test_without_required_skills_all_job_skills_count_as_required():
    result = score(job_skills=["go", "rust"], required_skills=[], candidate_skills=["go"],
                   min_experience_years=None, candidate_years=None)
    assert result["score"] == 35 + 10 + 10  # half the skills; no "others"; unknown experience = half


# ---------------------------------------------------------------- the activity

APP, CANDIDATE, JOB = "app-1", "cand-1", "job-1"


def fake_peers(resume_status="PARSED", job_found=True, candidate_down=False):
    def handler(request: httpx.Request) -> httpx.Response:
        if "/internal/candidates/" in request.url.path:
            if candidate_down:
                return httpx.Response(503)
            resume = None if resume_status is None else {
                "resume_id": "r1", "status": resume_status, "parse_error": None,
                "parsed_data": {"skills": ["python", "k8s"], "experience_years": 4.0}}
            return httpx.Response(200, json={"candidate_id": CANDIDATE, "experience_years": None, "resume": resume})
        if job_found:
            return httpx.Response(200, json={"job_id": JOB, "skills": ["python", "kubernetes", "sql"],
                                             "required_skills": ["python", "kubernetes"]})
        return httpx.Response(404, json={})

    return httpx.MockTransport(handler)


@pytest.fixture
async def stores(settings):
    client = AsyncMongoClient(settings.mongo_url)
    await client.drop_database(settings.mongo_database)
    await client.close()
    mongo.init_mongo(settings.mongo_url, settings.mongo_database)
    await scores.ensure_indexes()
    yield
    await peers.close_peers()
    await mongo.close_mongo()


async def run(**peer_options):
    peers.init_peers("http://candidate", "http://enrichment", transport=fake_peers(**peer_options))
    return await scoring.score_application(APP, CANDIDATE, JOB, 2.0)


async def test_scores_and_saves(stores):
    result = await run()
    assert result["score"] == 70 + 0 + 20  # both must-haves (k8s = kubernetes), no sql, 4 >= 2 years
    doc = await scores.get(APP)
    assert doc["job_id"] == JOB and doc["score"] == 90.0 and doc["semantic_score"] is None


async def test_running_twice_keeps_one_score(stores):
    await run()
    await run()  # the orchestrator re-ran the step after a lost answer
    assert await mongo.get_db()["scores"].count_documents({}) == 1


async def test_unparsed_resume_is_503_so_the_step_retries(stores):
    with pytest.raises(ServiceUnavailableError) as err:
        await run(resume_status="UPLOADED")
    assert err.value.code == "RESUME_NOT_PARSED"  # 5xx: parsing will catch up


@pytest.mark.parametrize("options", [{"resume_status": None}, {"resume_status": "FAILED"}, {"job_found": False}])
async def test_impossible_cases_are_422_and_not_retried(stores, options):
    with pytest.raises(AppError) as err:
        await run(**options)
    assert (err.value.http_status, err.value.code) == (422, "CANNOT_SCORE")
    assert await scores.get(APP) is None


async def test_peer_outage_is_503(stores):
    with pytest.raises(ServiceUnavailableError):
        await run(candidate_down=True)


def test_read_api_and_internal_endpoint(settings):
    from fastapi.testclient import TestClient
    from smarthire_common.testing import ServiceTokenFactory

    from app.main import create_app

    service_tokens = ServiceTokenFactory()
    with TestClient(create_app(settings, peer_transport=fake_peers(),
                               service_key_provider=service_tokens.key_provider)) as client:
        assert client.get("/scores/unknown").status_code == 404
        assert client.get("/health/ready").json()["checks"] == {"mongo": "ok"}
        body = {"application_id": APP, "candidate_id": CANDIDATE, "job_id": JOB, "min_experience_years": 2.0}
        assert client.post("/internal/scores", json=body, headers={"Idempotency-Key": "k"}).status_code == 401
        r = client.post("/internal/scores", json=body, headers=service_tokens.headers())
        assert r.status_code == 200 and r.json()["score"] == 90.0
        assert client.get(f"/scores/{APP}").json()["score"] == 90.0
