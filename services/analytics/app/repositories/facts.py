"""The facts table: insert once, read in time order for a rebuild."""

from datetime import datetime

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.facts import Fact
from app.models.projections import PROJECTION_TABLES


async def insert_fact(session: AsyncSession, *, fact_id: str, kind: str, occurred_at: datetime, data: dict) -> bool:
    """True if this fact is new; False if it was recorded before (a retried step, a replayed event)."""
    stmt = insert(Fact).values(fact_id=fact_id, kind=kind, occurred_at=occurred_at, data=data)
    result = await session.execute(stmt.on_conflict_do_nothing(index_elements=["fact_id"]).returning(Fact.fact_id))
    return result.scalar_one_or_none() is not None


async def lock_and_empty_projections(session: AsyncSession) -> None:
    # EXCLUSIVE blocks new facts (writers) but not readers until the rebuild commits, so a fact recorded
    # meanwhile can't be applied to a half-built read model and then applied again by the replay
    await session.execute(text("LOCK TABLE facts IN EXCLUSIVE MODE"))
    await session.execute(text(f"TRUNCATE {', '.join(PROJECTION_TABLES)}"))


async def all_in_time_order(session: AsyncSession):
    result = await session.execute(select(Fact.kind, Fact.data, Fact.occurred_at)
                                   .order_by(Fact.occurred_at, Fact.fact_id))
    return result.all()
