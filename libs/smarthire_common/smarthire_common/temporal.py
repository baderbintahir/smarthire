"""Temporal client and worker helpers (Phase 9). Needs the `temporal` extra: smarthire-common[temporal].

A service must start even while Temporal is down (like Kafka: the API keeps working). So the
worker connects in the background and keeps retrying, instead of failing the app's startup.

Observability (Phase 13): the client carries OpenTelemetry's TracingInterceptor, so starting a workflow
puts the caller's trace context in the workflow's headers, and every workflow run and activity becomes a
span in that same trace (workers built on this client inherit the interceptor). The SDK's own metrics
(workflows completed/failed per type, activity failures, schedule-to-start latency, ...) are served in
Prometheus format on `temporal_metrics_bind` (default :9464) by the SDK's Rust core."""

import asyncio
import logging
from collections.abc import Sequence

from temporalio.client import Client
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

log = logging.getLogger(__name__)

_client: Client | None = None
_runtime: Runtime | None = None
_lock = asyncio.Lock()


def _metrics_runtime(settings) -> Runtime:
    """One per process: it owns the metrics HTTP port."""
    global _runtime
    if _runtime is None:
        _runtime = Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(
            bind_address=settings.temporal_metrics_bind,
            durations_as_seconds=True,  # Prometheus convention (the default is milliseconds)
        )))
    return _runtime


async def connect(settings, *, retry_seconds: float = 5.0) -> Client:
    """The process-wide Temporal client, connecting (and retrying until it works) on first use."""
    global _client
    async with _lock:  # concurrent first callers share one connection attempt
        while _client is None:
            try:
                _client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace,
                                               interceptors=[TracingInterceptor()], runtime=_metrics_runtime(settings))
                log.info("connected to Temporal at %s", settings.temporal_address)
            except Exception as exc:
                log.warning("Temporal not reachable (%s); retrying in %ss", exc, retry_seconds)
                await asyncio.sleep(retry_seconds)
    return _client


def start_worker(settings, *, task_queue: str, workflows: Sequence = (), activities: Sequence = ()) -> asyncio.Task:
    """Run a worker for this task queue as a background task (cancel it on shutdown). A worker POLLS
    Temporal for tasks of its queue; nothing calls into this service, so it needs no open port."""

    async def run() -> None:
        while True:
            client = await connect(settings)
            try:
                log.info("Temporal worker started on %s", task_queue)
                await Worker(client, task_queue=task_queue, workflows=list(workflows), activities=list(activities),
                             max_concurrent_activities=settings.temporal_max_concurrent_activities).run()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Temporal worker on %s stopped; restarting in 5 s", task_queue)
                await asyncio.sleep(5)

    return asyncio.create_task(run(), name=f"temporal-worker-{task_queue}")


def reset_client() -> None:
    """Tests: forget the cached client."""
    global _client
    _client = None
