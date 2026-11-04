from contextlib import asynccontextmanager

from fastapi import FastAPI
from smarthire_common.background import stop_tasks
from smarthire_common.errors import register_exception_handlers
from smarthire_common.health import health_router, register_readiness_check
from smarthire_common.logging import setup_logging
from smarthire_common.metrics import setup_metrics
from smarthire_common.middleware import RequestIdMiddleware
from smarthire_common.mongo import close_mongo, init_mongo, mongo_ping_check
from smarthire_common.telemetry import flush_tracing, setup_tracing
from smarthire_common.temporal import start_worker

from app.api import jobs
from app.config import Settings
from app.repositories import structured_jobs
from app.workflows.activities import ACTIVITIES, TASK_QUEUE


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_mongo(settings.mongo_url, settings.mongo_database)
        await structured_jobs.ensure_indexes()
        register_readiness_check("mongo", mongo_ping_check)
        # The worker runs this service's activities when Job's workflow asks for them. It connects to
        # Temporal in the background (retrying), so the read API works even while Temporal is down
        tasks = [start_worker(settings, task_queue=TASK_QUEUE, activities=ACTIVITIES)] if settings.temporal_enabled else []
        yield
        await stop_tasks(*tasks)
        await close_mongo()
        flush_tracing()  # send spans still queued for Jaeger

    app = FastAPI(title=settings.service_name, lifespan=lifespan)
    setup_tracing(settings, app)  # spans for requests, SQL, Redis, outgoing HTTP (no-op unless enabled)
    app.state.settings = settings
    app.add_middleware(RequestIdMiddleware)
    register_exception_handlers(app)
    app.include_router(health_router)
    setup_metrics(app)  # GET /metrics + request count/latency
    app.include_router(jobs.router)
    return app
