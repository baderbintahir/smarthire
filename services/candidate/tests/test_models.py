"""Appendix E.6: the database itself enforces the Candidate rules. Each test breaks one rule and
checks the constraint that stops it (by its explicit name)."""

import uuid
from decimal import Decimal

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.models import Candidate, Resume
from app.models.candidate import DOCX, PDF


def make_resume(candidate_id: uuid.UUID, **overrides) -> Resume:
    fields = dict(
        candidate_id=candidate_id,
        object_key=f"{candidate_id}/{uuid.uuid4()}",
        file_name="cv.pdf",
        content_type=PDF,
        size_bytes=1024,
    )
    return Resume(**{**fields, **overrides})


async def add_candidate(session) -> uuid.UUID:
    candidate_id = uuid.uuid4()  # in real life: the Identity user id
    session.add(Candidate(id=candidate_id))
    await session.commit()
    return candidate_id


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


async def test_schema_names_match_appendix_e6_exactly(migrated, database_url):
    from smarthire_common.testing import schema_object_names

    assert await schema_object_names(database_url, "candidate") == {
        "pk_candidates", "ck_candidates_experience_years_non_negative",
        "pk_resumes", "fk_resumes_candidate_id_candidates", "uq_resumes_object_key",
        "ck_resumes_content_type", "ck_resumes_size_bytes", "ck_resumes_status",
        "uq_resumes_one_active", "ix_resumes_candidate_uploaded",
    }


# ---------- defaults ----------


async def test_defaults(session):
    candidate_id = await add_candidate(session)
    session.add(make_resume(candidate_id))
    await session.commit()
    row = (await session.execute(text("SELECT full_name, skills, education FROM candidates"))).one()
    assert tuple(row) == ("", [], [])
    resume = (await session.execute(text("SELECT status, is_active, uploaded_at FROM resumes"))).one()
    assert resume.status == "UPLOADED" and resume.is_active and resume.uploaded_at is not None


async def test_resumes_cascade_with_their_candidate(session):
    candidate_id = await add_candidate(session)
    session.add(make_resume(candidate_id))
    await session.commit()
    await session.execute(text("DELETE FROM candidates"))
    await session.commit()
    assert (await session.execute(text("SELECT count(*) FROM resumes"))).scalar_one() == 0


# ---------- rules enforced by constraints ----------


async def test_experience_cannot_be_negative(session):
    await expect_violation(
        session, "ck_candidates_experience_years_non_negative",
        Candidate(id=uuid.uuid4(), experience_years=Decimal("-1")),
    )


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"content_type": "image/png"}, "ck_resumes_content_type"),
        ({"content_type": "application/msword"}, "ck_resumes_content_type"),  # old .doc
        ({"size_bytes": 0}, "ck_resumes_size_bytes"),
        ({"size_bytes": 5 * 1024 * 1024 + 1}, "ck_resumes_size_bytes"),
        ({"status": "DELETED"}, "ck_resumes_status"),
    ],
)
async def test_resume_check_constraints(session, overrides, constraint):
    candidate_id = await add_candidate(session)
    await expect_violation(session, constraint, make_resume(candidate_id, **overrides))


async def test_both_allowed_types_and_the_exact_size_limit_are_accepted(session):
    candidate_id = await add_candidate(session)
    session.add_all([
        make_resume(candidate_id, content_type=PDF, size_bytes=5 * 1024 * 1024, is_active=False),
        make_resume(candidate_id, content_type=DOCX, size_bytes=1),
    ])
    await session.commit()


async def test_object_key_is_unique(session):
    candidate_id = await add_candidate(session)
    session.add(make_resume(candidate_id, object_key="same", is_active=False))
    await session.commit()
    await expect_violation(session, "uq_resumes_object_key", make_resume(candidate_id, object_key="same", is_active=False))


async def test_resume_needs_an_existing_candidate(session):
    await expect_violation(session, "fk_resumes_candidate_id_candidates", make_resume(uuid.uuid4()))


# ---------- the partial unique index: one ACTIVE resume per candidate ----------


async def test_only_one_active_resume_per_candidate(session):
    candidate_id = await add_candidate(session)
    session.add(make_resume(candidate_id))
    await session.commit()
    await expect_violation(session, "uq_resumes_one_active", make_resume(candidate_id))


async def test_any_number_of_inactive_resumes(session):
    candidate_id, other_id = await add_candidate(session), await add_candidate(session)
    session.add_all([make_resume(candidate_id, is_active=False) for _ in range(3)])
    session.add_all([make_resume(candidate_id), make_resume(other_id)])  # one active EACH is fine
    await session.commit()


async def test_deactivate_then_activate_in_one_transaction(session):
    """The order E.6 asks for: switching the active resume inside one transaction works."""
    candidate_id = await add_candidate(session)
    session.add(make_resume(candidate_id))
    await session.commit()
    await session.execute(text("UPDATE resumes SET is_active = false WHERE candidate_id = :c"), {"c": candidate_id})
    session.add(make_resume(candidate_id))
    await session.commit()
    active = await session.execute(text("SELECT count(*) FROM resumes WHERE is_active"))
    assert active.scalar_one() == 1


# ---------- indexes ----------


async def _plan(session, sql: str) -> str:
    # Tiny test tables make the planner prefer a full scan; disabling seq scans shows which index
    # the query CAN use
    await session.execute(text("SET LOCAL enable_seqscan = off"))
    return "\n".join((await session.execute(text(f"EXPLAIN {sql}"))).scalars().all())


async def test_resume_history_query_uses_its_index(session):
    plan = await _plan(
        session, f"SELECT id FROM resumes WHERE candidate_id = '{uuid.uuid4()}' ORDER BY uploaded_at DESC LIMIT 50"
    )
    assert "ix_resumes_candidate_uploaded" in plan


async def test_active_resume_query_uses_the_partial_index(session):
    plan = await _plan(session, f"SELECT id FROM resumes WHERE candidate_id = '{uuid.uuid4()}' AND is_active")
    assert "uq_resumes_one_active" in plan
