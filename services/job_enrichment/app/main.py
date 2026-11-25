from contextlib import asynccontextmanager

from fastapi import FastAPI
from smarthire_common.auth import JWKSClient, KeyProvider
from smarthire_common.errors import register_exception_handlers
from smarthire_common.health import health_router, register_readiness_check
from smarthire_common.logging import setup_logging
from smarthire_common.metrics import setup_metrics
from smarthire_common.middleware import RequestIdMiddleware
from smarthire_common.mongo import close_mongo, init_mongo, mongo_ping_check
from smarthire_common.telemetry import flush_tracing, setup_tracing
from smarthire_common.service_auth import close_service_auth, init_service_auth

from app.api import internal, jobs
from app.config import Settings
from app.repositories import structured_jobs


def create_app(settings: Settings | None = None, *, service_key_provider: KeyProvider | None = None) -> FastAPI:
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_mongo(settings.mongo_url, settings.mongo_database)
        await structured_jobs.ensure_indexes()
        register_readiness_check("mongo", mongo_ping_check)
        # Design v2.1: no Temporal worker here any more. The orchestrator's JobPublishingWorkflow calls
        # /internal/structured-jobs with its service token
        init_service_auth(service_key_provider or JWKSClient(settings.orchestrator_jwks_url))
        yield
        await close_service_auth()
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
    app.include_router(internal.router)
    return app
