"""Step 13.2: spans are created, a caller's trace is joined, health checks are skipped, logs carry ids.
Spans go to an in-memory exporter instead of Jaeger."""


import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from smarthire_common import telemetry
from smarthire_common.config import BaseServiceSettings
from smarthire_common.health import health_router
from smarthire_common.logging import _trace_ids

EXPORTER = InMemorySpanExporter()
CALLER_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"


@pytest.fixture(scope="module")
def client():
    settings = BaseServiceSettings(service_name="traced", tracing_enabled=True, _env_file=None)
    app = FastAPI()
    telemetry.setup_tracing(settings, app, exporter=EXPORTER)  # once per process (global provider)
    app.include_router(health_router)

    @app.get("/work")
    async def work() -> dict:
        return _trace_ids()  # what a log line written here would carry

    with TestClient(app) as c:
        yield c


def finished_spans() -> list:
    telemetry.flush_tracing()  # the batch processor exports in the background
    spans = list(EXPORTER.get_finished_spans())
    EXPORTER.clear()
    return spans


def test_a_request_creates_a_server_span_and_logs_carry_its_ids(client):
    ids = client.get("/work").json()
    server = [s for s in finished_spans() if s.kind.name == "SERVER"]
    assert [s.name for s in server] == ["GET /work"]
    assert ids["trace_id"] == format(server[0].context.trace_id, "032x")
    assert server[0].resource.attributes["service.name"] == "traced"


def test_the_callers_trace_is_joined(client):
    client.get("/work", headers={"traceparent": f"00-{CALLER_TRACE_ID}-00f067aa0ba902b7-01"})
    assert {format(s.context.trace_id, "032x") for s in finished_spans()} == {CALLER_TRACE_ID}


def test_health_checks_create_no_spans(client):
    client.get("/health/live")
    assert finished_spans() == []


def test_no_ids_outside_a_span():
    assert _trace_ids() == {"trace_id": None, "span_id": None}


async def run_sql(sql: str) -> None:
    from sqlalchemy import text

    from smarthire_common.db import close_db, init_db, session_scope

    init_db("sqlite+aiosqlite://")
    try:
        async with session_scope() as session:
            await session.execute(text(sql))
    finally:
        await close_db()


async def test_sql_statements_get_spans_inside_a_trace(client):  # `client` sets tracing up first
    with telemetry.get_tracer().start_as_current_span("handle an event", kind=SpanKind.CONSUMER):
        await run_sql("SELECT 42")
    spans = finished_spans()
    assert any("SELECT" in s.name for s in spans) and len({s.context.trace_id for s in spans}) == 1


async def test_background_sql_without_a_parent_is_not_traced(client):
    await run_sql("SELECT 43")  # like a retention job's query, with no request around it
    assert finished_spans() == []
