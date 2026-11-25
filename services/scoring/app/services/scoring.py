"""Use case: score one application (Step 10.3; design v2.1: the score_application step of the
orchestrator's ApplicationWorkflow, POST /internal/scores).

Errors are split in two, because the orchestrator treats them differently:
- RETRYABLE (503): the resume isn't parsed YET, Candidate/Enrichment is down. The step runs again later.
- BUSINESS (422 CANNOT_SCORE): no resume, unreadable resume, unknown job. Waiting won't change it; the
  workflow marks the score PENDING_MANUAL (a human looks at it).
Idempotent: the score is upserted by application id, so a repeated step keeps one score."""

import httpx
from smarthire_common.errors import AppError, ServiceUnavailableError

from app.domain.scoring import score
from app.repositories import peers, scores


def _cannot(reason: str) -> AppError:
    return AppError(reason, code="CANNOT_SCORE", http_status=422)


async def score_application(application_id: str, candidate_id: str, job_id: str,
                            min_experience_years: float | None = None) -> dict:
    try:
        facts = await peers.candidate_facts(candidate_id)
        resume = (facts or {}).get("resume")
        if resume is None:
            raise _cannot("the candidate has no resume")
        if resume["status"] == "UPLOADED":
            raise ServiceUnavailableError("resume not parsed yet", code="RESUME_NOT_PARSED")  # it will be soon
        if resume["status"] != "PARSED":
            raise _cannot(f"resume could not be parsed: {resume.get('parse_error')}")
        job = await peers.structured_job(job_id)
    except httpx.HTTPError as exc:
        raise ServiceUnavailableError(f"a peer service is unavailable: {exc}", code="PEER_UNAVAILABLE") from None
    if job is None:
        raise _cannot("no structured data for this job")

    parsed = resume["parsed_data"]
    years = [y for y in (parsed.get("experience_years"), facts.get("experience_years")) if y is not None]
    result = score(job_skills=job["skills"], required_skills=job.get("required_skills", []),
                   candidate_skills=parsed["skills"], min_experience_years=min_experience_years,
                   candidate_years=max(years) if years else None)
    await scores.save(application_id, {"job_id": job_id, "candidate_id": candidate_id, "score": result["score"],
                                       "rule_score": result["score"], "semantic_score": None,  # Phase 15
                                       "breakdown": result["breakdown"]})
    return result
