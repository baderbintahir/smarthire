"""Step 10.3: the score formula (pure), then the activity on a real MongoDB replica set with the
Candidate and Enrichment APIs replaced by httpx.MockTransport."""

import httpx
import pytest
from pymongo import AsyncMongoClient
from smarthire_common import mongo
from smarthire_common.mongo_outbox import MongoOutboxRelay, ensure_outbox_indexes
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from app.domain.scoring import score
from app.repositories import peers, scores
from app.workflows.activities import CANNOT_SCORE, score_application

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
    await ensure_outbox_indexes()
    yield
    await peers.close_peers()
    await mongo.close_mongo()


async def run(**peer_options):
    peers.init_peers("http://candidate", "http://enrichment", transport=fake_peers(**peer_options))
    return await ActivityEnvironment().run(score_application, APP, CANDIDATE, JOB, 2.0)


async def outbox_docs():
    return await mongo.get_db()["outbox"].find().to_list()


async def test_scores_saves_and_queues_candidate_scored_together(stores):
    result = await run()
    assert result["score"] == 70 + 0 + 20  # both must-haves (k8s = kubernetes), no sql, 4 >= 2 years
    doc = await scores.get(APP)
    assert doc["job_id"] == JOB and doc["score"] == 90.0 and doc["semantic_score"] is None
    [event] = await outbox_docs()
    assert event["event_type"] == "CandidateScored" and event["message_key"] == APP and event["published_at"] is None


async def test_running_twice_keeps_one_score_and_one_event(stores):
    await run()
    await run()  # Temporal re-ran it after a lost result
    assert await mongo.get_db()["scores"].count_documents({}) == 1
    assert len(await outbox_docs()) == 1


async def test_unparsed_resume_is_retryable(stores):
    with pytest.raises(ApplicationError) as err:
        await run(resume_status="UPLOADED")
    assert not err.value.non_retryable  # Temporal tries again: parsing will catch up


@pytest.mark.parametrize("options", [{"resume_status": None}, {"resume_status": "FAILED"}, {"job_found": False}])
async def test_impossible_cases_are_not_retried(stores, options):
    with pytest.raises(ApplicationError) as err:
        await run(**options)
    assert err.value.non_retryable and err.value.type == CANNOT_SCORE
    assert await scores.get(APP) is None and await outbox_docs() == []


async def test_peer_outage_raises_for_a_retry(stores):
    with pytest.raises(httpx.HTTPStatusError):  # a plain error: Temporal retries it
        await run(candidate_down=True)


async def test_mongo_outbox_relay_marks_what_kafka_confirmed(stores):
    await run()
    sent = []

    def fake_kafka(rows):
        sent.extend(r.event_type for r in rows)
        return [r.id for r in rows]

    assert await MongoOutboxRelay(fake_kafka).publish_batch() == (1, 1)
    assert sent == ["CandidateScored"] and (await outbox_docs())[0]["published_at"] is not None
    assert await MongoOutboxRelay(fake_kafka).publish_batch() == (0, 0)


def test_read_api(settings):
    from fastapi.testclient import TestClient

    from app.main import create_app

    with TestClient(create_app(settings)) as client:
        assert client.get("/scores/unknown").status_code == 404
        assert client.get("/health/ready").json()["checks"] == {"mongo": "ok"}
