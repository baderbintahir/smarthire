"""The Job lifecycle (Step 5.2): which status changes are allowed, and which actions are allowed
in which status. Pure functions, no database or framework: every rule is unit-tested directly,
and routers/services can't bypass it.

    DRAFT --publish--> PROCESSING --(Phase 9 workflow)--> READY --close--> CLOSED
                         |   ^                              |
                         v   | publish again                | edit (needs re-processing)
                        FAILED                              +--> PROCESSING
"""

from enum import StrEnum

from app.domain.errors import InvalidJobStagesError, JobNotEditableError, JobTransitionError


class JobStatus(StrEnum):
    DRAFT = "DRAFT"
    PROCESSING = "PROCESSING"  # the publishing workflow (Phase 9) is extracting/enriching it
    READY = "READY"  # published: visible on the public board, open for applications
    FAILED = "FAILED"  # publishing failed after all retries; the recruiter can fix and retry
    CLOSED = "CLOSED"  # final: no more applications


# The whole state machine in one table: current status -> statuses it may move to
ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.DRAFT: frozenset({JobStatus.PROCESSING}),
    JobStatus.PROCESSING: frozenset({JobStatus.READY, JobStatus.FAILED}),
    JobStatus.FAILED: frozenset({JobStatus.PROCESSING}),
    JobStatus.READY: frozenset({JobStatus.PROCESSING, JobStatus.CLOSED}),
    JobStatus.CLOSED: frozenset(),  # final
}

# Statuses a recruiter may edit in. PROCESSING: the workflow is reading this content right now.
# CLOSED: final.
EDITABLE = frozenset({JobStatus.DRAFT, JobStatus.FAILED, JobStatus.READY})

MAX_STAGES = 20


def transition(current: str, target: str) -> JobStatus:
    current, target = JobStatus(current), JobStatus(target)
    if target not in ALLOWED_TRANSITIONS[current]:
        raise JobTransitionError(current, target)
    return target


def status_after_edit(current: str) -> JobStatus:
    """Editing changes the content the publishing workflow processed, so a READY job has to be
    processed again (READY -> PROCESSING). DRAFT and FAILED stay as they are."""
    current = JobStatus(current)
    if current not in EDITABLE:
        raise JobNotEditableError(current)
    return transition(current, JobStatus.PROCESSING) if current is JobStatus.READY else current


def publish(current: str) -> JobStatus:
    """DRAFT or FAILED -> PROCESSING. Not READY: it's already published (edits re-process it)."""
    current = JobStatus(current)
    if current not in (JobStatus.DRAFT, JobStatus.FAILED):
        raise JobTransitionError(current, JobStatus.PROCESSING)
    return transition(current, JobStatus.PROCESSING)


def close(current: str) -> JobStatus:
    return transition(current, JobStatus.CLOSED)


def normalize_stages(names: list[str]) -> list[str]:
    """The hiring pipeline, in order. At least one stage, at most MAX_STAGES, no duplicates
    (ignoring case and surrounding spaces). Position = index in the list."""
    cleaned = [name.strip() for name in names]
    if not cleaned or len(cleaned) > MAX_STAGES:
        raise InvalidJobStagesError(f"A job needs between 1 and {MAX_STAGES} stages")
    if any(not name for name in cleaned):
        raise InvalidJobStagesError("Stage names can't be empty")
    if len({name.lower() for name in cleaned}) != len(cleaned):
        raise InvalidJobStagesError("Stage names must be unique")
    return cleaned


def normalize_skills(skills: list[str]) -> list[str]:
    """Lowercase, trimmed, de-duplicated, order kept: "Python", " python " and "PYTHON" are one skill."""
    seen: dict[str, None] = {}
    for skill in skills:
        if cleaned := skill.strip().lower():
            seen.setdefault(cleaned, None)
    return list(seen)
