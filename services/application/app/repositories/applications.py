"""All SQL for applications, their history, notes, quotas and audit entries."""

import base64
import json
import uuid
from datetime import datetime

from sqlalchemy import func, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.applications import ACTIVE
from app.models import Application, ApplicationNote, ApplicationQuota, AuditEntry, JobReplica, StageHistory

# ---------- quota: one lockable row per candidate ----------


async def lock_quota(session: AsyncSession, candidate_id: uuid.UUID) -> int:
    """Lock this candidate's quota row until commit and return their open-application count.

    The row is created on first use. Two first-ever applies at once: the second INSERT waits for the
    first transaction, then does nothing; both then queue on the FOR UPDATE lock, one at a time."""
    await session.execute(insert(ApplicationQuota).values(candidate_id=candidate_id).on_conflict_do_nothing())
    stmt = select(ApplicationQuota.active_count).where(ApplicationQuota.candidate_id == candidate_id).with_for_update()
    return (await session.execute(stmt)).scalar_one()


async def change_quota(session: AsyncSession, candidate_id: uuid.UUID, delta: int) -> None:
    # CHECK (active_count >= 0) turns a double decrement (a bug) into an error instead of bad data
    await session.execute(
        update(ApplicationQuota)
        .where(ApplicationQuota.candidate_id == candidate_id)
        .values(active_count=ApplicationQuota.active_count + delta)
    )


# ---------- writes ----------


async def add(session: AsyncSession, application: Application) -> None:
    session.add(application)
    await session.flush()  # the INSERT runs now: a duplicate (candidate, job) fails here


async def add_history(session: AsyncSession, application_id: uuid.UUID, *, from_stage: str | None, to_stage: str,
                      changed_by: uuid.UUID, reason: str | None = None) -> None:
    session.add(StageHistory(application_id=application_id, from_stage=from_stage, to_stage=to_stage,
                             changed_by=changed_by, reason=reason))
    await session.flush()


async def add_note(session: AsyncSession, note: ApplicationNote) -> None:
    session.add(note)
    await session.flush()


async def audit(session: AsyncSession, *, actor_id: uuid.UUID, action: str, entity_id: uuid.UUID,
                before: dict | None, after: dict | None) -> None:
    session.add(AuditEntry(actor_id=actor_id, action=action, entity_type="application", entity_id=entity_id,
                           before=before, after=after))
    await session.flush()


async def update_if(session: AsyncSession, application_id: uuid.UUID, *, version: int | None = None, **values) -> bool:
    """Conditional write: only an ACTIVE application (and, if given, at the version the client read)
    is changed. False = someone else got there first (decided, withdrew, or edited). The version
    goes up on every change."""
    conditions = [Application.id == application_id, Application.status.in_(ACTIVE)]
    if version is not None:
        conditions.append(Application.version == version)
    result = await session.execute(
        update(Application).where(*conditions).values(**values, version=Application.version + 1, updated_at=func.now())
    )
    return result.rowcount == 1


async def close_submitted(session: AsyncSession, job_id: uuid.UUID) -> list[tuple[uuid.UUID, uuid.UUID, str]]:
    """The job closed: its SUBMITTED applications (not yet screened) become CLOSED. Returns their
    (application_id, candidate_id, stage). Applications a recruiter is already working on stay open."""
    result = await session.execute(
        update(Application).where(Application.job_id == job_id, Application.status == "SUBMITTED")
        .values(status="CLOSED", version=Application.version + 1, updated_at=func.now())
        .returning(Application.id, Application.candidate_id, Application.current_stage)
    )
    return [(row.id, row.candidate_id, row.current_stage) for row in result]


# ---------- reads ----------


async def get_with_job(session: AsyncSession, application_id: uuid.UUID) -> tuple[Application, JobReplica] | None:
    stmt = select(Application, JobReplica).join(JobReplica).where(Application.id == application_id)
    row = (await session.execute(stmt)).first()
    return (row[0], row[1]) if row else None


async def history(session: AsyncSession, application_id: uuid.UUID) -> list[StageHistory]:
    # Served by ix_stage_history_app_changed
    stmt = select(StageHistory).where(StageHistory.application_id == application_id).order_by(StageHistory.changed_at, StageHistory.id)
    return list((await session.execute(stmt)).scalars())


async def notes(session: AsyncSession, application_id: uuid.UUID) -> list[ApplicationNote]:
    # Served by ix_application_notes_app_created
    stmt = select(ApplicationNote).where(ApplicationNote.application_id == application_id).order_by(ApplicationNote.created_at)
    return list((await session.execute(stmt)).scalars())


async def board(session: AsyncSession, job_id: uuid.UUID, *, stage: str | None, limit: int, offset: int) -> list[Application]:
    """The recruiter board, best scores first, unscored last. Served by
    ix_applications_job_stage_score (job_id, current_stage, score DESC NULLS LAST)."""
    stmt = select(Application).where(Application.job_id == job_id)
    if stage:
        stmt = stmt.where(Application.current_stage == stage)
    stmt = stmt.order_by(Application.score.desc().nulls_last(), Application.id).limit(limit).offset(offset)
    return list((await session.execute(stmt)).scalars())


# Keyset pagination for "my applications" (same idea as Job's board): (created_at, id) < cursor


def encode_cursor(application: Application) -> str:
    return base64.urlsafe_b64encode(json.dumps([application.created_at.isoformat(), str(application.id)]).encode()).decode()


def _decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    created_at, app_id = json.loads(base64.urlsafe_b64decode(cursor.encode()))
    return datetime.fromisoformat(created_at), uuid.UUID(app_id)


async def for_candidate(session: AsyncSession, candidate_id: uuid.UUID, *, limit: int, cursor: str | None
                        ) -> tuple[list[tuple[Application, JobReplica]], str | None]:
    # Served by ix_applications_candidate_created (candidate_id, created_at DESC)
    stmt = select(Application, JobReplica).join(JobReplica).where(Application.candidate_id == candidate_id)
    if cursor:
        stmt = stmt.where(tuple_(Application.created_at, Application.id) < _decode_cursor(cursor))
    rows = (await session.execute(stmt.order_by(Application.created_at.desc(), Application.id.desc()).limit(limit + 1))).all()
    page = [(r[0], r[1]) for r in rows[:limit]]
    return page, encode_cursor(page[-1][0]) if len(rows) > limit else None
