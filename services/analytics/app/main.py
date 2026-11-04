from contextlib import asynccontextmanager

from fastapi import FastAPI
from smarthire_common.auth import JWKSClient, KeyProvider, close_auth, init_auth
from smarthire_common.background import stop_tasks
from smarthire_common.db import close_db, init_db
from smarthire_common.errors import register_exception_handlers
from smarthire_common.health import db_ping_check, health_router, register_readiness_check
from smarthire_common.kafka import start_consumer
from smarthire_common.logging import setup_logging
from smarthire_common.metrics import setup_metrics
from smarthire_common.middleware import RequestIdMiddleware
from smarthire_common.redis import close_redis, init_redis, redis_ping_check
from smarthire_common.telemetry import flush_tracing, setup_tracing

from app.api import metrics
from app.config import Settings
from app.events import handlers


def create_app(settings: Settings | None = None, *, key_provider: KeyProvider | None = None) -> FastAPI:
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db(settings.database_url)
        init_redis(settings.redis_url)  # token denylist + metrics cache
        init_auth(key_provider or JWKSClient(settings.jwks_url), issuer=settings.jwt_issuer,
                  audience=settings.jwt_audience)
        register_readiness_check("database", db_ping_check)
        register_readiness_check("redis", redis_ping_check)
        # The read model is fed ONLY by events (CQRS): this consumer is the one writer
        tasks = []
        if settings.kafka_enabled:
            tasks = [start_consumer(settings, name=handlers.CONSUMER_NAME, topics=handlers.TOPICS,
                                    handlers=handlers.HANDLERS)]
        yield
        await stop_tasks(*tasks)
        await close_auth()
        await close_redis()
        await close_db()
        flush_tracing()  # send spans still queued for Jaeger

    app = FastAPI(title=settings.service_name, lifespan=lifespan)
    setup_tracing(settings, app)  # spans for requests, SQL, Redis, outgoing HTTP (no-op unless enabled)
    app.state.settings = settings
    app.add_middleware(RequestIdMiddleware)
    register_exception_handlers(app)
    app.include_router(health_router)
    setup_metrics(app)  # GET /metrics + request count/latency
    app.include_router(metrics.router)
    return app
