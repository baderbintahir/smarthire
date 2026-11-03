"""The scores collection (Appendix E.11): _id = application_id, so scoring an application twice (a
Temporal retry) replaces its one document instead of adding another."""

from datetime import UTC, datetime

from pymongo import DESCENDING, IndexModel
from pymongo.asynchronous.client_session import AsyncClientSession
from smarthire_common.mongo import get_db

COLLECTION = "scores"


def _collection():
    return get_db()[COLLECTION]


async def ensure_indexes() -> None:
    # "Best candidates for this job" (analytics, the assistant): WHERE job_id ORDER BY score DESC
    await _collection().create_indexes([IndexModel([("job_id", 1), ("score", DESCENDING)], name="ix_job_score")])


async def save(application_id: str, doc: dict, *, session: AsyncClientSession) -> bool:
    """Insert or replace. True when this application had no score before (its first scoring)."""
    result = await _collection().replace_one(
        {"_id": application_id}, {**doc, "scored_at": datetime.now(UTC)}, upsert=True, session=session
    )
    return result.upserted_id is not None


async def get(application_id: str) -> dict | None:
    return await _collection().find_one({"_id": application_id})
