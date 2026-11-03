"""Pure rules: no database, no network."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.domain import applications as rules
from app.domain.errors import (
    ApplicationNotActiveError,
    InvalidStageError,
    JobNotOpenError,
    NotEligibleError,
    ProfileIncompleteError,
    QuotaExceededError,
)

NOW = datetime(2026, 10, 30, 12, tzinfo=UTC)
STAGES = ["Screening", "Interview", "Offer"]


def job(status="READY", deadline=None, rules_=None) -> rules.JobFacts:
    return rules.JobFacts(status=status, deadline=deadline, eligibility_rules=rules_ or {}, stages=STAGES)


def candidate(name="Jane Doe", years="5", resume=True) -> rules.CandidateFacts:
    return rules.CandidateFacts(full_name=name, experience_years=Decimal(years) if years is not None else None,
                                has_resume=resume)


# ---------- job open ----------


def test_ready_job_before_deadline_is_open():
    rules.check_job_open(job(deadline=NOW + timedelta(seconds=1)), NOW)


@pytest.mark.parametrize("facts", [None, job("DRAFT"), job("PROCESSING"), job("CLOSED"), job(deadline=NOW)])
def test_unknown_unpublished_closed_or_past_deadline_jobs_are_not_open(facts):
    with pytest.raises(JobNotOpenError):
        rules.check_job_open(facts, NOW)


# ---------- profile + eligibility ----------


@pytest.mark.parametrize("facts", [candidate(name="  "), candidate(resume=False)])
def test_incomplete_profiles_cannot_apply(facts):
    with pytest.raises(ProfileIncompleteError):
        rules.check_profile(facts)


@pytest.mark.parametrize(("years", "ok"), [("2", True), ("2.0", True), ("1.9", False), (None, False)])
def test_min_experience(years, ok):
    failures = rules.eligibility_failures({"min_experience_years": 2}, candidate(years=years))
    assert (failures == []) is ok


def test_no_rules_means_everyone_is_eligible():
    rules.check_eligible({}, candidate(years=None))


def test_not_eligible_lists_the_reasons():
    with pytest.raises(NotEligibleError) as err:
        rules.check_eligible({"min_experience_years": 10}, candidate(years="3"))
    assert err.value.details == {"reasons": ["at least 10 years of experience required"]}


# ---------- quota ----------


def test_quota_allows_up_to_the_limit():
    rules.check_quota(2, limit=3)
    with pytest.raises(QuotaExceededError):
        rules.check_quota(3, limit=3)


# ---------- stage changes and decisions ----------


def test_stage_must_belong_to_the_job_and_differ():
    rules.check_stage_change("SUBMITTED", "SUBMITTED", "Interview", STAGES)
    rules.check_stage_change("IN_PROGRESS", "Offer", "Screening", STAGES)  # moving back is allowed
    with pytest.raises(InvalidStageError):
        rules.check_stage_change("IN_PROGRESS", "Interview", "Lunch", STAGES)
    with pytest.raises(InvalidStageError):
        rules.check_stage_change("IN_PROGRESS", "Interview", "Interview", STAGES)


@pytest.mark.parametrize("status", ["HIRED", "REJECTED", "WITHDRAWN", "CLOSED"])
def test_final_applications_cannot_change(status):
    with pytest.raises(ApplicationNotActiveError):
        rules.check_stage_change(status, "Interview", "Offer", STAGES)
    with pytest.raises(ApplicationNotActiveError):
        rules.check_decision(status, "HIRED")
