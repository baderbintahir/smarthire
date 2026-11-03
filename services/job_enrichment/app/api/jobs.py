"""Read API (internal: other services call it; not routed through the gateway).

TODO(Phase 10): service-to-service authentication when Scoring starts calling this."""

from fastapi import APIRouter, Query
from smarthire_common.errors import NotFoundError

from app.repositories import structured_jobs
from app.schemas.structured_job import SearchResults, StructuredJobOut

router = APIRouter(tags=["structured jobs"])


@router.get("/jobs/{job_id}/structured", response_model=StructuredJobOut)
async def get_structured(job_id: str):
    doc = await structured_jobs.latest(job_id)
    if doc is None:
        raise NotFoundError("No structured data for this job", code="STRUCTURED_JOB_NOT_FOUND")
    return StructuredJobOut.from_doc(doc)


@router.get("/search/jobs", response_model=SearchResults)
async def search_jobs(q: str = Query(min_length=2, max_length=200), limit: int = Query(20, ge=1, le=50)):
    return SearchResults(items=[StructuredJobOut.from_doc(doc) for doc in await structured_jobs.search(q, limit)])
