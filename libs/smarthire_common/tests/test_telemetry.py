"""Step 13.2: spans are created, a caller's trace is joined, health checks are skipped, logs carry ids.
Spans go to an in-memory exporter instead of Jaeger."""

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from prometheus_client import REGISTRY

from smarthire_common import telemetry
from smarthire_common.config import BaseServiceSettings
from smarthire_common.events import new_event
from smarthire_common.health import health_router
from smarthire_common.kafka import EventConsumer
from smarthire_common.logging import _trace_ids
from smarthire_common.outbox import KafkaSender

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
    await run_sql("SELECT 43")  # like the outbox relay's poll every 200 ms
    assert finished_spans() == []


def test_new_events_carry_the_current_trace_context(client):
    with telemetry.get_tracer().start_as_current_span("request", kind=SpanKind.SERVER) as span:
        event = new_event("UserRegistered", producer="test", payload={"user_id": "u", "role": "CANDIDATE"})
    assert event.trace_context["traceparent"].split("-")[1] == format(span.get_span_context().trace_id, "032x")
    finished_spans()
    assert new_event("UserRegistered", producer="t", payload={}).trace_context == {}  # outside a span


def test_relay_publish_continues_the_events_trace_and_puts_its_own_span_in_the_headers(client):
    with telemetry.get_tracer().start_as_current_span("request", kind=SpanKind.SERVER):
        event = new_event("UserRegistered", producer="test", payload={"user_id": "u", "role": "CANDIDATE"})
    sender = object.__new__(KafkaSender)  # no Kafka: fake producer and serializer
    sender._serialize = lambda event_type, payload, topic: b"value"
    sender._producer = producer = FakeProducer()
    row = SimpleNamespace(id=1, topic="identity.users", message_key="u", event_type="UserRegistered",
                          payload=event.model_dump(mode="json"), headers=event.trace_context)
    assert sender([row]) == [1]
    publish = next(s for s in finished_spans() if s.name == "publish identity.users")
    assert publish.kind == SpanKind.PRODUCER and publish.parent is not None
    traceparent = dict(producer.headers)["traceparent"]
    assert traceparent.split("-")[2] == format(publish.context.span_id, "016x")  # consumers continue from here


async def test_consumer_continues_the_trace_from_the_message_headers(client):
    seen = []

    async def handler(session, event):
        seen.append(_trace_ids()["trace_id"])

    consumer = EventConsumer(name="c", topics=["identity.users"], handlers={"UserRegistered": handler},
                             bootstrap_servers="unused", schema_registry_url="unused")
    event = new_event("UserRegistered", producer="test", payload={"user_id": "u", "role": "CANDIDATE"})
    consumer._deserialize = lambda msg: event.model_dump(mode="json")

    async def fake_process(envelope, handler):
        await handler(None, envelope)
        return True

    consumer.process = fake_process
    headers = [("traceparent", f"00-{CALLER_TRACE_ID}-00f067aa0ba902b7-01".encode())]
    await consumer.handle_message(SimpleNamespace(headers=lambda: headers, topic=lambda: "identity.users"))
    span = next(s for s in finished_spans() if s.kind == SpanKind.CONSUMER)
    assert span.name == "process UserRegistered" and seen == [CALLER_TRACE_ID]
    assert span.attributes["messaging.message.id"] == str(event.event_id) and span.attributes["outcome"] == "processed"
    assert REGISTRY.get_sample_value("smarthire_events_consumed_total",
                                     {"consumer": "c", "event_type": "UserRegistered", "outcome": "processed"}) == 1


class FakeProducer:
    def produce(self, topic, key, value, headers, on_delivery):
        self.headers = headers
        self._callback = on_delivery

    def flush(self, _timeout):
        self._callback(None, None)
