"""Distributed tracing with OpenTelemetry (Step 13.2).

A TRACE is one request's whole journey; it is made of SPANS (timed steps: "POST /applications",
"SELECT ...", "GET http://candidate:8004/..."). Auto-instrumentation creates spans for the libraries
we use, without touching our code:
  - FastAPI: a span per incoming request; READS the W3C `traceparent` header (joins the caller's trace)
  - httpx: a span per outgoing call; WRITES `traceparent` (so the next service joins this trace)
  - SQLAlchemy, Redis, PyMongo: a span per statement / command
Finished spans are batched and sent over OTLP/HTTP to Jaeger in a background thread: a request never
waits for (or fails because of) the tracing backend.

Asynchronous hops (Step 13.3) carry the same W3C context: Kafka in message headers (kafka.py, outbox.py),
Celery in task headers (Celery instrumentation), Temporal in workflow/activity headers (temporal.py's
TracingInterceptor). So one trace follows a request through events, workflows and background tasks."""

import logging

from fastapi import FastAPI
from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.sampling import Decision, ParentBased, Sampler, SamplingResult
from opentelemetry.trace import SpanKind

from smarthire_common.config import BaseServiceSettings

logger = logging.getLogger(__name__)

# Called every few seconds by Prometheus and Docker healthchecks: spans for them would bury real traces
EXCLUDED_URLS = "health/.*,metrics"

_provider: TracerProvider | None = None  # one per process (OTel's global provider can be set only once)


class _SkipBackgroundRoots(Sampler):
    """Decides for spans WITHOUT a parent. A trace may START with work arriving (an HTTP request, a
    Kafka message, a Celery task: SERVER/CONSUMER/PRODUCER spans). A parentless CLIENT/INTERNAL span is
    background chatter (the outbox relay's SELECT every 200 ms, readiness pings): dropped, or Jaeger
    would be buried in one-span traces. Spans WITH a parent follow it (ParentBased below)."""

    def should_sample(self, parent_context, trace_id, name, kind=None, attributes=None, links=None, trace_state=None):
        if kind in (SpanKind.SERVER, SpanKind.CONSUMER, SpanKind.PRODUCER):
            return SamplingResult(Decision.RECORD_AND_SAMPLE, attributes)
        return SamplingResult(Decision.DROP)

    def get_description(self) -> str:
        return "SkipBackgroundRoots"


def setup_tracing(settings: BaseServiceSettings, app: FastAPI | None = None, *,
                  exporter: SpanExporter | None = None, service_name: str | None = None) -> None:
    """Call in create_app (before the lifespan creates DB/Redis/Mongo clients), or with no app in a
    Celery worker process. No-op unless settings.tracing_enabled. `service_name` overrides the
    settings' (workers: "<service>-worker"). `exporter` lets tests collect spans in memory."""
    global _provider
    if not settings.tracing_enabled:
        return
    if _provider is None:
        if exporter is None:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            exporter = OTLPSpanExporter(endpoint=settings.otlp_endpoint)
        # service.name is how Jaeger groups spans ("Service" dropdown in the UI)
        _provider = TracerProvider(resource=Resource.create({"service.name": service_name or settings.service_name}),
                                   sampler=ParentBased(root=_SkipBackgroundRoots()))
        _provider.add_span_processor(BatchSpanProcessor(exporter))  # queue + background export
        trace.set_tracer_provider(_provider)
        _instrument_libraries()
    if app is not None:
        FastAPIInstrumentor.instrument_app(app, excluded_urls=EXCLUDED_URLS, tracer_provider=_provider)


def _instrument_libraries() -> None:
    # These patch library CLASSES, so clients created later (in the lifespan) are traced
    HTTPXClientInstrumentor().instrument()
    RedisInstrumentor().instrument()
    try:  # only services with the `mongo` extra have PyMongo (and its instrumentation)
        from opentelemetry.instrumentation.pymongo import PymongoInstrumentor

        PymongoInstrumentor().instrument()  # a global command listener: applies to clients created after
    except ImportError:
        pass
    try:  # `celery` extra. Sending side: puts traceparent in the task headers; worker side: continues it
        from opentelemetry.instrumentation.celery import CeleryInstrumentor

        CeleryInstrumentor().instrument()
    except ImportError:
        pass


def trace_engine(engine) -> None:
    """SQLAlchemy is traced per ENGINE (db.init_db calls this). No-op while tracing is off."""
    if _provider is not None:
        instrumentor = SQLAlchemyInstrumentor()  # a singleton: instrument() is ignored when already done
        if instrumentor.is_instrumented_by_opentelemetry:
            instrumentor.uninstrument()  # init_db ran again (new engine): move tracing to it
        # skip_dep_check: the instrumentation declares "sqlalchemy < 2.1" and otherwise refuses to run on
        # our 2.1. It only uses engine events (before/after_cursor_execute), unchanged in 2.1: verified live.
        instrumentor.instrument(engine=engine.sync_engine, tracer_provider=_provider, skip_dep_check=True)


def flush_tracing() -> None:
    """Call on shutdown: send the spans still queued (bounded wait, never hangs the shutdown)."""
    if _provider is not None:
        _provider.force_flush(timeout_millis=5000)


def get_tracer():
    """For hand-made spans (Kafka publish/process, SMTP). A no-op tracer while tracing is off."""
    return trace.get_tracer("smarthire")


def current_context_headers() -> dict[str, str]:
    """The current trace context as W3C headers ({"traceparent": ...}); {} outside a span."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


def context_from_headers(headers: dict[str, str] | None) -> Context:
    """The reverse: a parent context to start a span in (an empty context if there's none)."""
    return propagate.extract(headers or {})
