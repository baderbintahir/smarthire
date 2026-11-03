"""Use case: parse one resume (Step 10.2). Called by the Celery task in app/tasks/parsing.py.

Idempotent, because Celery may deliver the task more than once (acks_late, retries, duplicate
enqueues): only an UPLOADED resume is parsed, and the final UPDATE checks that again, so a second run
changes nothing and writes no second ResumeParsed."""

import logging
import uuid
from datetime import UTC, datetime

from smarthire_common.db import session_scope
from smarthire_common.events import new_event
from smarthire_common.outbox import add_outbox_event

from app.domain.resume_parser import ResumeUnreadableError, extract_text, parse
from app.repositories import candidates as repo
from app.repositories import files

log = logging.getLogger(__name__)


async def parse_resume(resume_id: uuid.UUID) -> str:
    """Returns what happened: "parsed", "failed" (unreadable file), or "skipped" (nothing to do)."""
    async with session_scope() as session:
        resume = await repo.get_resume(session, resume_id)
        if resume is None or resume.status != "UPLOADED":
            return "skipped"  # unknown, or parsed/failed already: a duplicate task
        key, content_type, candidate_id = resume.object_key, resume.content_type, resume.candidate_id

    data = await files.get(key)  # outside any transaction: no DB connection held during the download
    try:
        parsed = parse(extract_text(data, content_type), datetime.now(UTC).date())
    except ResumeUnreadableError as exc:
        # A bad FILE is not a temporary problem: record it, don't retry
        async with session_scope() as session:
            await repo.mark_resume_failed(session, resume_id, str(exc))
        log.warning("resume %s could not be parsed: %s", resume_id, exc)
        return "failed"

    async with session_scope() as session:
        if not await repo.mark_resume_parsed(session, resume_id, parsed):
            return "skipped"  # another run finished first
        event = new_event("ResumeParsed", producer="candidate", payload={
            "candidate_id": str(candidate_id), "resume_id": str(resume_id),
            "skills": parsed["skills"], "experience_years": parsed["experience_years"],
        })
        await add_outbox_event(session, event, key=str(candidate_id))  # same transaction as PARSED
    return "parsed"
