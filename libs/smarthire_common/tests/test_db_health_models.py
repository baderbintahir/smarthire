import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import CheckConstraint, ForeignKey, Index, String, func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.schema import CreateIndex, CreateTable

from smarthire_common import health
from smarthire_common.db import close_db, get_engine, init_db, session_scope
from smarthire_common.models import Base, TimestampMixin, new_id


class Widget(Base, TimestampMixin):
    __tablename__ = "widgets"
    __table_args__ = (
        CheckConstraint("length(name) > 0", name="name_not_empty"),
        Index("ix_widgets_name", "name"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(50), unique=True)


class Part(Base):
    __tablename__ = "parts"
    id: Mapped[int] = mapped_column(primary_key=True)
    widget_id: Mapped[int] = mapped_column(ForeignKey("widgets.id"))


# ---------- db.py: commit / rollback ----------


@pytest.fixture
async def db():
    init_db("sqlite+aiosqlite:///:memory:")
    async with get_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await close_db()


async def _count() -> int:
    async with session_scope() as s:
        return (await s.execute(select(func.count()).select_from(Widget))).scalar_one()


async def test_session_commits_on_success(db):
    async with session_scope() as s:
        s.add(Widget(name="a"))
    assert await _count() == 1


async def test_session_rolls_back_on_exception(db):
    with pytest.raises(ValueError):
        async with session_scope() as s:
            s.add(Widget(name="a"))
            await s.flush()  # row is written inside the transaction...
            raise ValueError("boom")
    assert await _count() == 0  # ...but gone after rollback


# ---------- health.py ----------


@pytest.fixture
def client():
    health.clear_readiness_checks()
    app = FastAPI()
    app.include_router(health.health_router)
    yield TestClient(app)
    health.clear_readiness_checks()


def test_live_checks_nothing(client):
    async def always_fails():
        raise RuntimeError("db down")

    health.register_readiness_check("db", always_fails)
    assert client.get("/health/live").json() == {"status": "ok"}  # liveness ignores checks


def test_ready_ok(client):
    async def ok():
        return None

    health.register_readiness_check("db", ok)
    r = client.get("/health/ready")
    assert r.status_code == 200
    assert r.json()["checks"] == {"db": "ok"}


def test_ready_503_when_check_fails(client):
    async def bad():
        raise RuntimeError("password=hunter2")

    health.register_readiness_check("db", bad)
    r = client.get("/health/ready")
    assert r.status_code == 503
    assert r.json()["checks"] == {"db": "fail"}
    assert "hunter2" not in r.text


def test_ready_503_when_check_times_out(client, monkeypatch):
    monkeypatch.setattr(health, "CHECK_TIMEOUT_SECONDS", 0.05)

    async def slow():
        await asyncio.sleep(1)

    health.register_readiness_check("slow", slow)
    assert client.get("/health/ready").status_code == 503


# ---------- models.py ----------


def test_naming_convention_names_constraints_and_indexes():
    ddl = str(CreateTable(Widget.__table__).compile(dialect=postgresql.dialect()))
    assert "CONSTRAINT pk_widgets PRIMARY KEY (id)" in ddl
    assert "CONSTRAINT uq_widgets_name UNIQUE (name)" in ddl
    assert "CONSTRAINT ck_widgets_name_not_empty CHECK (length(name) > 0)" in ddl
    assert "TIMESTAMP WITH TIME ZONE" in ddl and "DEFAULT now()" in ddl
    fk_ddl = str(CreateTable(Part.__table__).compile(dialect=postgresql.dialect()))
    assert "CONSTRAINT fk_parts_widget_id_widgets FOREIGN KEY(widget_id)" in fk_ddl
    ix = next(i for i in Widget.__table__.indexes)
    assert "ix_widgets_name" in str(CreateIndex(ix).compile(dialect=postgresql.dialect()))


def test_new_id_is_uuid7_and_time_ordered():
    ids = [new_id() for _ in range(50)]
    assert all(i.version == 7 for i in ids)
    assert [i.int for i in ids] == sorted(i.int for i in ids)
