"""Appendix E.5: the database itself enforces the Job rules. Each test breaks one rule and
checks the constraint that stops it (by its explicit name)."""

import uuid

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.models import Job, JobStage


def make_job(**overrides) -> Job:
    fields = dict(
        recruiter_id=uuid.uuid4(),
        title="Backend Engineer",
        description="Build APIs",
        employment_type="FULL_TIME",
        required_skills=["python", "postgresql"],
    )
    return Job(**{**fields, **overrides})


async def expect_violation(session, constraint: str, *objects) -> None:
    session.add_all(objects)
    with pytest.raises(IntegrityError) as err:
        await session.commit()
    assert constraint in str(err.value)  # the named constraint did the rejecting
    await session.rollback()


# ---------- migrations (Appendix E.3) ----------


def test_upgrade_downgrade_upgrade(migrated):
    command.downgrade(migrated, "base")
    command.upgrade(migrated, "head")


def test_models_match_migrations(migrated):
    command.check(migrated)  # like `alembic check`: raises if models and migrations differ


# ---------- defaults ----------


async def test_defaults(session):
    job = make_job()
    session.add(job)
    await session.commit()
    row = (await session.execute(text("SELECT status, version, eligibility_rules FROM jobs"))).one()
    assert tuple(row) == ("DRAFT", 1, {})
    assert job.id.version == 7  # UUIDv7 primary key


async def test_stages_belong_to_their_job_and_cascade(session):
    job = make_job()
    session.add(job)
    await session.flush()
    session.add_all([JobStage(job_id=job.id, name="Screening", position=0),
                     JobStage(job_id=job.id, name="Interview", position=1)])
    await session.commit()
    await session.delete(job)
    await session.commit()
    assert (await session.execute(text("SELECT count(*) FROM job_stages"))).scalar_one() == 0


# ---------- rules enforced by constraints ----------


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"title": "AB"}, "ck_jobs_title_length"),
        ({"title": "x" * 201}, "ck_jobs_title_length"),
        ({"employment_type": "FREELANCE"}, "ck_jobs_employment_type"),
        ({"application_limit": 0}, "ck_jobs_application_limit_positive"),
        ({"status": "PUBLISHED"}, "ck_jobs_status"),
    ],
)
async def test_job_check_constraints(session, overrides, constraint):
    await expect_violation(session, constraint, make_job(**overrides))


async def test_stage_names_and_positions_unique_per_job(session):
    job, other = make_job(), make_job()
    session.add_all([job, other])
    await session.flush()
    # Plain variables: after a rollback, ORM objects are stale, and reloading them implicitly
    # is not allowed in async code (MissingGreenlet)
    job_id, other_id = job.id, other.id
    session.add(JobStage(job_id=job_id, name="Screening", position=0))
    await session.commit()

    await expect_violation(session, "uq_job_stages_job_name", JobStage(job_id=job_id, name="Screening", position=1))
    await expect_violation(session, "uq_job_stages_job_position", JobStage(job_id=job_id, name="Interview", position=0))
    await expect_violation(session, "ck_job_stages_position_non_negative",
                           JobStage(job_id=job_id, name="Offer", position=-1))
    # The same name/position in ANOTHER job is fine: uniqueness is per job
    session.add(JobStage(job_id=other_id, name="Screening", position=0))
    await session.commit()


# ---------- indexes ----------


async def _plan(session, sql: str) -> str:
    # Tiny test tables make the planner prefer a full scan; disabling seq scans shows which index
    # the query CAN use
    await session.execute(text("SET LOCAL enable_seqscan = off"))
    rows = (await session.execute(text(f"EXPLAIN {sql}"))).scalars().all()
    return "\n".join(rows)


async def test_public_board_query_uses_partial_index(session):
    plan = await _plan(session, "SELECT id FROM jobs WHERE status = 'READY' ORDER BY created_at DESC LIMIT 20")
    assert "ix_jobs_ready_created" in plan


async def test_partial_index_cannot_serve_other_statuses(session):
    plan = await _plan(session, "SELECT id FROM jobs WHERE status = 'DRAFT' ORDER BY created_at DESC LIMIT 20")
    assert "ix_jobs_ready_created" not in plan  # drafts aren't in it


async def test_recruiter_query_uses_composite_index(session):
    rid = uuid.uuid4()
    plan = await _plan(
        session,
        f"SELECT id FROM jobs WHERE recruiter_id = '{rid}' AND status = 'DRAFT' ORDER BY created_at DESC LIMIT 20",
    )
    assert "ix_jobs_recruiter_status_created" in plan


async def test_schema_names_match_appendix_e5_exactly(migrated, database_url):
    from smarthire_common.testing import schema_object_names

    assert await schema_object_names(database_url, "job") == {
        "pk_jobs", "pk_job_stages", "fk_job_stages_job_id_jobs",
        "ck_jobs_title_length", "ck_jobs_employment_type", "ck_jobs_application_limit_positive",
        "ck_jobs_status", "ck_job_stages_position_non_negative",
        "uq_job_stages_job_position", "uq_job_stages_job_name",
        "ix_jobs_recruiter_status_created", "ix_jobs_ready_created",
    }
