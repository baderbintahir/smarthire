"""Activities (run in Temporal's ActivityEnvironment, no server needed) and the read API, on a real
MongoDB. The key property: every activity can run twice without changing the result."""

from temporalio.testing import ActivityEnvironment

from app.repositories import structured_jobs
from app.workflows import activities
from tests.test_domain import DESCRIPTION

JOB = "01a1b2c3-0000-7000-8000-000000000001"


async def run(activity_fn, *args):
    return await ActivityEnvironment().run(activity_fn, *args)


async def enriched(version=1) -> dict:
    extracted = await run(activities.extract_skills, JOB, version, DESCRIPTION, ["python"])
    return {**extracted, "sections": await run(activities.breakdown_jd, DESCRIPTION)}


async def count() -> int:
    from smarthire_common.mongo import get_db

    return await get_db()[structured_jobs.COLLECTION].count_documents({})


async def test_store_is_an_idempotent_upsert(mongo):
    data = await enriched()
    await run(activities.store_structured_job, JOB, 1, data)
    await run(activities.store_structured_job, JOB, 1, data)  # a Temporal retry
    assert await count() == 1  # same _id "<job_id>:<version>": rewritten, not duplicated
    doc = await structured_jobs.latest(JOB)
    assert doc["_id"] == f"{JOB}:1" and doc["skills"][0] == "python" and doc["required_skills"] == ["python"]


async def test_versions_are_separate_documents_and_latest_wins(mongo):
    await run(activities.store_structured_job, JOB, 1, await enriched(1))
    await run(activities.store_structured_job, JOB, 2, await enriched(2))
    assert await count() == 2 and (await structured_jobs.latest(JOB))["version"] == 2


async def test_compensation_deletes_and_is_safe_to_repeat(mongo):
    await run(activities.store_structured_job, JOB, 1, await enriched())
    await run(activities.delete_structured_job, JOB, 1)
    await run(activities.delete_structured_job, JOB, 1)  # nothing left to delete: still fine
    assert await count() == 0


def test_read_api(client):
    from smarthire_common import testing

    async def store():
        # Its OWN connection: init_mongo()/close_mongo() here would swap the one the running app uses
        from datetime import UTC, datetime

        from pymongo import AsyncMongoClient

        settings = client.app.state.settings
        mongo = AsyncMongoClient(settings.mongo_url)
        data = await enriched(3)
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
