"""Prometheus metrics (Steps 13.1, 13.4). Prometheus PULLS ("scrapes") GET /metrics every 15 s.

Three kinds: a COUNTER only goes up (graph its rate), a GAUGE goes up and down (a current level), a
HISTOGRAM counts values into buckets (latency percentiles). Every metric the chassis records is
defined here, so the names dashboards use live in one place. Prometheus adds `service` (from the
scrape target, see infra/prometheus/prometheus.yml), so no metric carries a service label itself.

One uvicorn process per container: the default in-process registry is the whole picture."""

import time

from fastapi import APIRouter, FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# ---------------------------------------------------------------- HTTP (RED: rate, errors, duration)
HTTP_REQUESTS = Counter("http_requests_total", "HTTP requests handled", ["method", "route", "status"])
HTTP_DURATION = Histogram("http_request_duration_seconds", "HTTP request duration", ["method", "route"])

# ---------------------------------------------------------------- events (Kafka, outbox)
OUTBOX_PUBLISHED = Counter("smarthire_outbox_published_total", "Outbox events confirmed by Kafka", ["topic"])
OUTBOX_BACKLOG = Gauge("smarthire_outbox_backlog", "Outbox events not yet published")
OUTBOX_OLDEST_AGE = Gauge("smarthire_outbox_oldest_age_seconds", "Age of the oldest unpublished outbox event")
EVENTS_CONSUMED = Counter("smarthire_events_consumed_total", "Events handled by a consumer group",
                          ["consumer", "event_type", "outcome"])  # processed | duplicate | ignored | dead_lettered
CONSUMER_LAG = Gauge("smarthire_consumer_lag", "Messages in a partition not yet processed by the group",
                     ["consumer", "topic", "partition"])
DLQ_MESSAGES = Counter("smarthire_dlq_messages_total", "Messages sent to a dead-letter topic", ["consumer", "topic"])

# ---------------------------------------------------------------- workflows (the Temporal SDK exports the rest)
WORKFLOWS_STARTED = Counter("smarthire_workflows_started_total", "Workflows started", ["workflow_type"])

metrics_router = APIRouter(tags=["metrics"])


@metrics_router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


class HttpMetricsMiddleware:
    """Counts and times every request. Labels use the ROUTE TEMPLATE (/jobs/{job_id}), never the raw
    path: one label value per URL would create a new time series per job id and swamp Prometheus."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http" or scope["path"] == "/metrics" or scope["path"].startswith("/health/"):
            await self.app(scope, receive, send)  # scrapes and probes would drown the real traffic
            return
        status = 500  # if the app raises before answering, the server turns it into a 500

        async def capture_status(message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        started = time.perf_counter()
        try:
            await self.app(scope, receive, capture_status)
        finally:
            route = getattr(scope.get("route"), "path", "unmatched")  # FastAPI puts the matched route here
            HTTP_REQUESTS.labels(scope["method"], route, str(status)).inc()
            HTTP_DURATION.labels(scope["method"], route).observe(time.perf_counter() - started)


def setup_metrics(app: FastAPI) -> None:
    """Call in create_app: GET /metrics + request metrics."""
    app.include_router(metrics_router)
    app.add_middleware(HttpMetricsMiddleware)
