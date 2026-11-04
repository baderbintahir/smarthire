from contextlib import asynccontextmanager

from fastapi import FastAPI
from smarthire_common.background import stop_tasks
from smarthire_common.db import close_db, init_db
from smarthire_common.errors import register_exception_handlers
from smarthire_common.health import db_ping_check, health_router, register_readiness_check
from smarthire_common.kafka import start_consumer
from smarthire_common.logging import setup_logging
from smarthire_common.metrics import setup_metrics
from smarthire_common.middleware import RequestIdMiddleware
from smarthire_common.retention import start_retention
from smarthire_common.telemetry import flush_tracing, setup_tracing

from app import maintenance
from app.config import Settings
from app.events import handlers
from app.repositories.identity_api import IdentityClient


def create_app(settings: Settings | None = None) -> FastAPI:
    """No public API: this process consumes events and queues emails; the worker container sends them."""
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db(settings.database_url, pool_size=settings.db_pool_size, max_overflow=settings.db_max_overflow)
        identity = IdentityClient(settings.identity_url)
        register_readiness_check("database", db_ping_check)
        tasks = []
        if settings.kafka_enabled:
            tasks = [start_consumer(settings, name=handlers.CONSUMER_NAME, topics=handlers.TOPICS,
                                    handlers=handlers.build_handlers(settings, identity))]
        tasks += start_retention(maintenance.JOBS)  # E.14 clean-up, daily (Step 14.3b)
        yield
        await stop_tasks(*tasks)
        await identity.close()
        await close_db()
        flush_tracing()  # send spans still queued for Jaeger

    app = FastAPI(title=settings.service_name, lifespan=lifespan)
    setup_tracing(settings, app)  # spans for requests, SQL, Redis, outgoing HTTP (no-op unless enabled)
    app.state.settings = settings
    app.add_middleware(RequestIdMiddleware)
    register_exception_handlers(app)
    app.include_router(health_router)
    setup_metrics(app)  # GET /metrics + request count/latency
    return app
