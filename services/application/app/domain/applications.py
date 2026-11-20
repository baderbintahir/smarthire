"""Application rules (pure functions, no I/O): who may apply, how an application moves.

Lifecycle:
  SUBMITTED --(scored, Phase 10)--> SCREENING --(recruiter moves it to a job stage)--> IN_PROGRESS
  any ACTIVE --> HIRED | REJECTED (recruiter decision) | WITHDRAWN (candidate) | CLOSED (job closed)
The last four are final. Only ACTIVE applications count against the candidate's quota."""

import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from app.domain.errors import (
    ApplicationNotActiveError,
    InvalidStageError,
    JobNotOpenError,
    NotEligibleError,
    OwnJobError,
    ProfileIncompleteError,
    QuotaExceededError,
)

ACTIVE = frozenset({"SUBMITTED", "SCREENING", "IN_PROGRESS"})
DECISIONS = frozenset({"HIRED", "REJECTED"})
FIRST_STAGE = "SUBMITTED"  # current_stage when an application is created


@dataclass(frozen=True)
class CandidateFacts:
    """What we know about the applicant at apply time (from the Candidate service). Not the name:
    that comes from the token, Identity being its only source (design v1.1)."""

    experience_years: Decimal | None
    has_resume: bool


@dataclass(frozen=True)
class JobFacts:
    """What we know about the job (from our job_replica)."""

    status: str
    deadline: datetime | None
    eligibility_rules: dict
    stages: list[str]


def check_job_open(job: JobFacts | None, now: datetime) -> None:
    if job is None or job.status != "READY":
        raise JobNotOpenError("This job is not accepting applications")
    if job.deadline is not None and now >= job.deadline:
        raise JobNotOpenError("The application deadline has passed")


def check_not_own_job(job_recruiter_id: uuid.UUID, applicant_id: uuid.UUID) -> None:
    if job_recruiter_id == applicant_id:
        raise OwnJobError()


def check_profile(candidate: CandidateFacts, name: str | None) -> None:
    """name: the token's `name` claim (Identity's users.full_name)."""
    missing = []
    if not (name or "").strip():
        missing.append("your full name")  # only tokens from before design v1.1 lack it: log in again
    if not candidate.has_resume:
        missing.append("a resume")  # Scoring (Phase 10) needs one
    if missing:
        raise ProfileIncompleteError(f"Add {' and '.join(missing)} to your profile before applying")


def eligibility_failures(rules: dict, candidate: CandidateFacts) -> list[str]:
    """Every rule the candidate fails (all of them, so the message lists everything at once)."""
    failures = []
    minimum = rules.get("min_experience_years")
    if minimum is not None and (candidate.experience_years is None or candidate.experience_years < Decimal(str(minimum))):
        failures.append(f"at least {minimum} years of experience required")
    return failures


def check_eligible(rules: dict, candidate: CandidateFacts) -> None:
    if failures := eligibility_failures(rules, candidate):
        raise NotEligibleError(failures)


def check_quota(active_count: int, limit: int) -> None:
    if active_count >= limit:
        raise QuotaExceededError(limit)


def check_active(status: str) -> None:
    if status not in ACTIVE:
        raise ApplicationNotActiveError(status)


def check_stage_change(status: str, current_stage: str, to_stage: str, job_stages: list[str]) -> None:
    """Recruiters move an active application to one of THIS job's stages (forward or back)."""
    check_active(status)
    if to_stage not in job_stages:
        raise InvalidStageError(f"{to_stage!r} is not a stage of this job; stages: {', '.join(job_stages)}")
    if to_stage == current_stage:
        raise InvalidStageError(f"The application is already in {to_stage!r}")


def check_decision(status: str, decision: str) -> None:
    check_active(status)
    if decision not in DECISIONS:
        raise InvalidStageError(f"Decision must be one of {', '.join(sorted(DECISIONS))}")
