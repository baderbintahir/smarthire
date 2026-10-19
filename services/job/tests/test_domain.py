"""Step 5.2: the state machine, tested without a database. Every pair of statuses is checked, so
an accidental new arrow in ALLOWED_TRANSITIONS fails a test."""

import itertools

import pytest

from app.domain import job
from app.domain.errors import InvalidJobStagesError, JobNotEditableError, JobTransitionError
from app.domain.job import JobStatus as S

# The plan's allowed transitions (Step 5.2), written out independently of the code
EXPECTED = {
    (S.DRAFT, S.PROCESSING),
    (S.PROCESSING, S.READY),
    (S.PROCESSING, S.FAILED),
    (S.FAILED, S.PROCESSING),
    (S.READY, S.PROCESSING),
    (S.READY, S.CLOSED),
}


@pytest.mark.parametrize(("current", "target"), list(itertools.product(S, S)))
def test_every_transition(current, target):
    if (current, target) in EXPECTED:
        assert job.transition(current, target) is target
    else:
        with pytest.raises(JobTransitionError):
            job.transition(current, target)


def test_closed_is_final():
    assert job.ALLOWED_TRANSITIONS[S.CLOSED] == frozenset()


@pytest.mark.parametrize(("current", "after"), [(S.DRAFT, S.DRAFT), (S.FAILED, S.FAILED), (S.READY, S.PROCESSING)])
def test_edit_allowed_and_ready_needs_reprocessing(current, after):
    assert job.status_after_edit(current) is after


@pytest.mark.parametrize("current", [S.PROCESSING, S.CLOSED])
def test_edit_forbidden_while_processing_or_closed(current):
    with pytest.raises(JobNotEditableError):
        job.status_after_edit(current)


@pytest.mark.parametrize("current", [S.DRAFT, S.FAILED])
def test_publish_from_draft_or_failed(current):
    assert job.publish(current) is S.PROCESSING


@pytest.mark.parametrize("current", [S.PROCESSING, S.READY, S.CLOSED])
def test_publish_rejected_otherwise(current):
    with pytest.raises(JobTransitionError):
        job.publish(current)


def test_close_only_from_ready():
    assert job.close(S.READY) is S.CLOSED
    for current in (S.DRAFT, S.PROCESSING, S.FAILED, S.CLOSED):
        with pytest.raises(JobTransitionError):
            job.close(current)


def test_stage_rules():
    assert job.normalize_stages([" Screening ", "Interview", "Offer"]) == ["Screening", "Interview", "Offer"]
    for bad in ([], ["A"] * 21, ["Screening", "screening "], ["Screening", "  "]):
        with pytest.raises(InvalidJobStagesError):
            job.normalize_stages(bad)


def test_skills_normalized():
    assert job.normalize_skills(["Python", " python ", "PostgreSQL", "", "FastAPI"]) == ["python", "postgresql", "fastapi"]
