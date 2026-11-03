"""Activities on task queue `enrichment-tq` (Appendix B), called by Job's JobPublishingWorkflow.

Temporal may run an activity MORE THAN ONCE: it retries after errors and timeouts, and an activity
that finished but whose worker died before reporting back is run again. So each activity is
idempotent: running it twice has the same effect as once (pure functions, upserts by a fixed _id,
delete-if-present). The names are the contract with the workflow, which calls them by name."""

from temporalio import activity

from app.domain.breakdown import breakdown
from app.domain.skills import extract_keywords, normalize
from app.domain.skills import extract_skills as find_skills
from app.repositories import structured_jobs

TASK_QUEUE = "enrichment-tq"


@activity.defn(name="extract_skills")
async def extract_skills(job_id: str, version: int, description: str, required_skills: list[str]) -> dict:
    activity.logger.info("extracting skills for job %s v%s", job_id, version)
    return {
        "skills": find_skills(description, required_skills),  # everything: required first, then found in text
        "required_skills": list(dict.fromkeys(normalize(s) for s in required_skills if s.strip())),
        "keywords": extract_keywords(description),
    }


@activity.defn(name="breakdown_jd")
async def breakdown_jd(description: str) -> dict:
    return breakdown(description)


@activity.defn(name="store_structured_job")
async def store_structured_job(job_id: str, version: int, data: dict) -> None:
    await structured_jobs.upsert(job_id, version, data)


@activity.defn(name="delete_structured_job")
async def delete_structured_job(job_id: str, version: int) -> None:
    """Compensation: undo store_structured_job when the workflow fails after it."""
    await structured_jobs.delete(job_id, version)


ACTIVITIES = [extract_skills, breakdown_jd, store_structured_job, delete_structured_job]
