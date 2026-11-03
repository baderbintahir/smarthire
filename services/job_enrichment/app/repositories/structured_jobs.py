"""The structured_jobs collection (Appendix E.11).

_id is "<job_id>:<version>": deterministic. Storing the same job version twice (a Temporal retry
after a timeout, a replayed workflow) rewrites the SAME document instead of adding a second one."""

from datetime import UTC, datetime

from pymongo import DESCENDING, TEXT, IndexModel
from smarthire_common.mongo import get_db

COLLECTION = "structured_jobs"


def _collection():
    return get_db()[COLLECTION]


def doc_id(job_id: str, version: int) -> str:
    return f"{job_id}:{version}"


async def ensure_indexes() -> None:
    """Idempotent: creating an index that already exists is a no-op."""
    await _collection().create_indexes([
        IndexModel([("job_id", 1), ("version", DESCENDING)], name="ix_job_version"),  # latest version of a job
        # Full-text search over keywords and sections (one text index per collection is allowed)
        IndexModel([("keywords", TEXT), ("skills", TEXT), ("sections.requirements", TEXT),
                    ("sections.responsibilities", TEXT), ("sections.benefits", TEXT)], name="tx_search"),
    ])


async def upsert(job_id: str, version: int, data: dict) -> None:
    await _collection().replace_one(
        {"_id": doc_id(job_id, version)},
        {"job_id": job_id, "version": version, "skills": data["skills"],
         "required_skills": data.get("required_skills", []), "keywords": data["keywords"],
         "sections": data["sections"], "created_at": datetime.now(UTC)},
        upsert=True,  # insert if missing, replace if present: same result however many times it runs
    )


async def delete(job_id: str, version: int) -> None:
    await _collection().delete_one({"_id": doc_id(job_id, version)})  # deleting twice is fine too


async def latest(job_id: str) -> dict | None:
    return await _collection().find_one({"job_id": job_id}, sort=[("version", DESCENDING)])


async def search(query: str, limit: int = 20) -> list[dict]:
    """Text search, best match first, one result per job (its newest matching version)."""
    pipeline = [
        {"$match": {"$text": {"$search": query}}},
        {"$addFields": {"relevance": {"$meta": "textScore"}}},
        {"$sort": {"job_id": 1, "version": -1}},
        {"$group": {"_id": "$job_id", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$sort": {"relevance": -1, "job_id": 1}},
        {"$limit": limit},
    ]
    return await (await _collection().aggregate(pipeline)).to_list()
