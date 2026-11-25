"""The workflow steps (design v2.1: internal endpoints the orchestrator calls) and the read API, on a real
MongoDB. The key property: every step can run twice without changing the result."""

from app.repositories import structured_jobs
from app.services import enrichment
from tests.test_domain import DESCRIPTION

JOB = "01a1b2c3-0000-7000-8000-000000000001"


async def count() -> int:
    from smarthire_common.mongo import get_db

    return await get_db()[structured_jobs.COLLECTION].count_documents({})


async def test_build_is_an_idempotent_upsert(mongo):
    first = await enrichment.build(JOB, 1, DESCRIPTION, ["Python"])
    assert await enrichment.build(JOB, 1, DESCRIPTION, ["Python"]) == first  # a Temporal retry
    assert await count() == 1  # same _id "<job_id>:<version>": rewritten, not duplicated
    doc = await structured_jobs.latest(JOB)
    assert doc["_id"] == f"{JOB}:1" and doc["skills"][0] == "python" and doc["required_skills"] == ["python"]
    assert doc["sections"]["benefits"]


async def test_versions_are_separate_documents_and_latest_wins(mongo):
    await enrichment.build(JOB, 1, DESCRIPTION, ["python"])
    await enrichment.build(JOB, 2, DESCRIPTION, ["python"])
    assert await count() == 2 and (await structured_jobs.latest(JOB))["version"] == 2


async def test_compensation_deletes_and_is_safe_to_repeat(mongo):
    await enrichment.build(JOB, 1, DESCRIPTION, ["python"])
    await enrichment.remove(JOB, 1)
    await enrichment.remove(JOB, 1)  # nothing left to delete: still fine
    assert await count() == 0


def test_internal_endpoints_need_the_orchestrator(client, service_tokens):
    body = {"job_id": JOB, "version": 7, "description": DESCRIPTION, "required_skills": ["python"]}
    assert client.post("/internal/structured-jobs", json=body, headers={"Idempotency-Key": "k"}).status_code == 401
    r = client.post("/internal/structured-jobs", json=body, headers=service_tokens.headers())
    assert r.status_code == 200 and r.json()["version"] == 7
    assert client.get(f"/jobs/{JOB}/structured").json()["version"] == 7
    assert client.delete(f"/internal/structured-jobs/{JOB}/7", headers=service_tokens.headers()).status_code == 204
    assert client.get(f"/jobs/{JOB}/structured").status_code == 404


def test_read_api(client):
    from smarthire_common import testing

    async def store():
        # Its OWN connection: init_mongo()/close_mongo() here would swap the one the running app uses
        from datetime import UTC, datetime

        from pymongo import AsyncMongoClient

        settings = client.app.state.settings
        mongo = AsyncMongoClient(settings.mongo_url)
        from app.domain.breakdown import breakdown
        from app.domain.skills import extract_skills

        data = {"skills": extract_skills(DESCRIPTION, ["python"]), "required_skills": ["python"], "keywords": [],
                "sections": breakdown(DESCRIPTION)}
        await mongo[settings.mongo_database][structured_jobs.COLLECTION].insert_one(
            {"_id": f"{JOB}:3", "job_id": JOB, "version": 3, **data, "created_at": datetime.now(UTC)})
        await mongo.close()

    assert client.get(f"/jobs/{JOB}/structured").status_code == 404
    testing.run(store())
    body = client.get(f"/jobs/{JOB}/structured").json()
    assert body["version"] == 3 and "kafka" in body["skills"] and body["sections"]["benefits"]

    hits = client.get("/search/jobs", params={"q": "kafka pipeline"}).json()["items"]
    assert [h["job_id"] for h in hits] == [JOB] and hits[0]["relevance"] > 0
    assert client.get("/search/jobs", params={"q": "haskell"}).json()["items"] == []
    assert client.get("/search/jobs", params={"q": "x"}).status_code == 422  # too short


def test_readiness_checks_mongo(client):
    assert client.get("/health/ready").json()["checks"] == {"mongo": "ok"}
