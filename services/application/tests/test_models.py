"""Appendix E.7: the database itself enforces the Application rules. Each test breaks one rule and
checks the constraint that stops it (by its explicit name)."""

import uuid

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.models import Application, ApplicationNote, ApplicationQuota, JobReplica


def make_job(**overrides) -> JobReplica:
    fields = dict(job_id=uuid.uuid4(), recruiter_id=uuid.uuid4(), title="Backend Engineer", status="READY",
                  stages=["Screening", "Interview"], source_version=1)
    return JobReplica(**{**fields, **overrides})


def make_application(job_id, **overrides) -> Application:
    fields = dict(job_id=job_id, candidate_id=uuid.uuid4(), candidate_name="Jane Doe", current_stage="SUBMITTED")
    return Application(**{**fields, **overrides})


async def add_job(session) -> uuid.UUID:
    job = make_job()
    session.add(job)
    await session.commit()
    return job.job_id


async def expect_violation(session, constraint: str, *objects) -> None:
    session.add_all(objects)
    with pytest.raises(IntegrityError) as err:
        await session.commit()
    assert constraint in str(err.value)
    await session.rollback()


# ---------- migrations ----------


def test_upgrade_downgrade_upgrade(migrated):
    command.downgrade(migrated, "base")
    command.upgrade(migrated, "head")


def test_models_match_migrations(migrated):
    command.check(migrated)


async def test_schema_names_match_appendix_e7_exactly(migrated, database_url):
    from smarthire_common.testing import schema_object_names

    assert await schema_object_names(database_url, "application") == {
        "pk_job_replica",
        "pk_applications", "fk_applications_job_id_job_replica", "uq_applications_candidate_job",
        "ck_applications_status", "ck_applications_score_range", "ck_applications_score_status",
        "ix_applications_job_stage_score", "ix_applications_candidate_created",
        "pk_application_stage_history", "fk_application_stage_history_application_id_applications",
        "ix_stage_history_app_changed",
        "pk_application_notes", "fk_application_notes_application_id_applications",
        "ck_application_notes_body_not_empty", "ix_application_notes_app_created",
        "pk_application_quotas", "ck_application_quotas_active_count_non_negative",
        "pk_audit_entries", "ix_audit_entries_entity",
        "pk_outbox", "uq_outbox_event_id", "ix_outbox_unpublished", "pk_processed_events",
    }


# ---------- rules enforced by constraints ----------


async def test_defaults(session):
    job_id = await add_job(session)
    session.add(make_application(job_id))
    await session.commit()
    row = (await session.execute(text("SELECT status, score_status, version, score FROM applications"))).one()
    assert tuple(row) == ("SUBMITTED", "PENDING", 1, None)


async def test_one_application_per_candidate_and_job(session):
    job_id, candidate_id = await add_job(session), uuid.uuid4()
    session.add(make_application(job_id, candidate_id=candidate_id))
    await session.commit()
    await expect_violation(session, "uq_applications_candidate_job", make_application(job_id, candidate_id=candidate_id))


async def test_application_needs_a_known_job(session):
    await expect_violation(session, "fk_applications_job_id_job_replica", make_application(uuid.uuid4()))


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"status": "ACCEPTED"}, "ck_applications_status"),
        ({"score": 100.5}, "ck_applications_score_range"),
        ({"score": -1}, "ck_applications_score_range"),
        ({"score_status": "DONE"}, "ck_applications_score_status"),
    ],
)
async def test_application_check_constraints(session, overrides, constraint):
    job_id = await add_job(session)
    await expect_violation(session, constraint, make_application(job_id, **overrides))


async def test_quota_cannot_go_negative(session):
    await expect_violation(session, "ck_application_quotas_active_count_non_negative",
                           ApplicationQuota(candidate_id=uuid.uuid4(), active_count=-1))


async def test_note_body_cannot_be_empty(session):
    job_id = await add_job(session)
    application = make_application(job_id)
    session.add(application)
    await session.commit()
    await expect_violation(session, "ck_application_notes_body_not_empty",
                           ApplicationNote(application_id=application.id, recruiter_id=uuid.uuid4(), body=""))


# ---------- indexes ----------


async def _plan(session, sql: str) -> str:
    await session.execute(text("SET LOCAL enable_seqscan = off"))
    return "\n".join((await session.execute(text(f"EXPLAIN {sql}"))).scalars().all())


async def test_board_query_uses_the_board_index(session):
    plan = await _plan(session, f"SELECT id FROM applications WHERE job_id = '{uuid.uuid4()}' AND current_stage = 'Interview' "
                                "ORDER BY score DESC NULLS LAST LIMIT 50")
    assert "ix_applications_job_stage_score" in plan


async def test_my_applications_query_uses_its_index(session):
    plan = await _plan(session, f"SELECT id FROM applications WHERE candidate_id = '{uuid.uuid4()}' "
                                "ORDER BY created_at DESC LIMIT 20")
    assert "ix_applications_candidate_created" in plan
