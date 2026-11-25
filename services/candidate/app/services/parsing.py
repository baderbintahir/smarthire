"""Use case: parse one resume (Step 10.2; design v2.1: the parse_resume step of ResumeProcessingWorkflow,
POST /internal/resumes/{id}/parse, served by the candidate-parser container).

Idempotent, because the step may run more than once (Temporal retries after a timeout): only an
UPLOADED resume is parsed, and the final UPDATE checks that again, so a second run changes nothing."""

import asyncio
import logging
import uuid
from datetime import UTC, datetime

from smarthire_common.db import session_scope

from app.domain.resume_parser import ResumeUnreadableError, extract_text, parse
from app.repositories import candidates as repo
from app.repositories import files

log = logging.getLogger(__name__)


async def parse_resume(resume_id: uuid.UUID) -> str:
    """Returns what happened: "parsed", "failed" (unreadable file), "skipped" (done before), "missing"."""
    async with session_scope() as session:
        resume = await repo.get_resume(session, resume_id)
        if resume is None:
            return "missing"
        if resume.status != "UPLOADED":
            return "skipped"  # parsed/failed already: a repeated step
        key, content_type, candidate_id = resume.object_key, resume.content_type, resume.candidate_id

    data = await files.get(key)  # outside any transaction: no DB connection held during the download
    try:
        # CPU-bound: in a thread, so this process keeps answering health checks meanwhile
        text = await asyncio.to_thread(extract_text, data, content_type)
        parsed = await asyncio.to_thread(parse, text, datetime.now(UTC).date())
    except ResumeUnreadableError as exc:
        # A bad FILE is not a temporary problem: record it, don't retry
        async with session_scope() as session:
            await repo.mark_resume_failed(session, resume_id, str(exc))
        log.warning("resume %s could not be parsed: %s", resume_id, exc)
        return "failed"

    async with session_scope() as session:
        if not await repo.mark_resume_parsed(session, resume_id, parsed):
            return "skipped"  # another run finished first
    log.info("resume %s of candidate %s parsed", resume_id, candidate_id)
    return "parsed"
