"""Activity on task queue `scoring-tq` (Appendix B), called by Application's ApplicationWorkflow.

Errors are split in two, because Temporal treats them differently:
- RETRYABLE: the resume isn't parsed YET, Candidate/Enrichment is down. Temporal tries again later.
- NON-RETRYABLE (non_retryable=True): no resume at all, unreadable resume, unknown job. Waiting won't
  change it; the workflow marks the score PENDING_MANUAL (a human looks at it)."""

from smarthire_common.events import new_event
from smarthire_common.mongo import get_client
from smarthire_common.mongo_outbox import add_outbox_event
from temporalio import activity
from temporalio.exceptions import ApplicationError

from app.domain.scoring import score
from app.repositories import peers, scores

TASK_QUEUE = "scoring-tq"
CANNOT_SCORE = "CannotScore"  # the workflow recognises this type


def _cannot(reason: str) -> ApplicationError:
    return ApplicationError(reason, type=CANNOT_SCORE, non_retryable=True)


@activity.defn(name="score_application")
async def score_application(application_id: str, candidate_id: str, job_id: str,
                            min_experience_years: float | None = None) -> dict:
    facts = await peers.candidate_facts(candidate_id)
    resume = (facts or {}).get("resume")
    if resume is None:
        raise _cannot("the candidate has no resume")
    if resume["status"] == "UPLOADED":
        # Parsing (a Celery task) hasn't finished yet: a plain error, so Temporal retries with back-off
        raise ApplicationError("resume not parsed yet", type="ResumeNotParsed")
    if resume["status"] != "PARSED":
        raise _cannot(f"resume could not be parsed: {resume.get('parse_error')}")
    job = await peers.structured_job(job_id)
    if job is None:
        raise _cannot("no structured data for this job")

    parsed = resume["parsed_data"]
    years = [y for y in (parsed.get("experience_years"), facts.get("experience_years")) if y is not None]
    result = score(job_skills=job["skills"], required_skills=job.get("required_skills", []),
                   candidate_skills=parsed["skills"], min_experience_years=min_experience_years,
                   candidate_years=max(years) if years else None)

    doc = {"job_id": job_id, "candidate_id": candidate_id, "score": result["score"], "rule_score": result["score"],
           "semantic_score": None, "breakdown": result["breakdown"]}  # semantic_score: Phase 15
    # ONE Mongo transaction: the score and its CandidateScored event are saved together or not at all
    async with get_client().start_session() as session:
        async with await session.start_transaction():
            if await scores.save(application_id, doc, session=session):  # first time: tell others
                event = new_event("CandidateScored", producer="scoring", payload={
                    "application_id": application_id, "job_id": job_id, "candidate_id": candidate_id,
                    "score": result["score"], "breakdown": result["breakdown"]})
                await add_outbox_event(event, key=application_id, session=session)
    return result
