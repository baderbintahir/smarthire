"""Use case: turn a job's text into a structured job (Step 9.3; design v2.1: the build_structured_job step
of the orchestrator's JobPublishingWorkflow, POST /internal/structured-jobs).

Idempotent: pure functions, then an upsert by a fixed _id ("<job_id>:<version>"), so running the step
twice (Temporal retries after a timeout) stores the same document once."""

from app.domain.breakdown import breakdown
from app.domain.skills import extract_keywords, normalize
from app.domain.skills import extract_skills as find_skills
from app.repositories import structured_jobs


async def build(job_id: str, version: int, description: str, required_skills: list[str]) -> dict:
    data = {
        "skills": find_skills(description, required_skills),  # everything: required first, then found in text
        "required_skills": list(dict.fromkeys(normalize(s) for s in required_skills if s.strip())),
        "keywords": extract_keywords(description),
        "sections": breakdown(description),
    }
    await structured_jobs.upsert(job_id, version, data)
    return {"job_id": job_id, "version": version, "skills": data["skills"]}


async def remove(job_id: str, version: int) -> None:
    """Compensation: the publishing failed after this version's document was stored."""
    await structured_jobs.delete(job_id, version)
