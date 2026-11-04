from contextlib import asynccontextmanager

from fastapi import FastAPI
from smarthire_common.auth import StaticKeyProvider, close_auth, init_auth
from smarthire_common.background import stop_tasks
from smarthire_common.db import close_db, init_db
from smarthire_common.errors import register_exception_handlers
from smarthire_common.health import db_ping_check, health_router, register_readiness_check
from smarthire_common.logging import setup_logging
from smarthire_common.metrics import setup_metrics
from smarthire_common.middleware import RequestIdMiddleware
from smarthire_common.outbox import start_outbox_relay
from smarthire_common.redis import close_redis, init_redis, redis_ping_check
from smarthire_common.retention import start_retention
from smarthire_common.telemetry import flush_tracing, setup_tracing

from app import maintenance
from app.api import auth, internal, jwks, users
from app.config import Settings
from app.domain.tokens import load_signing_key


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Load the key first: a missing/invalid key should stop startup, not fail every login
        key = app.state.signing_key = load_signing_key(settings.jwt_private_key_path)
        # Verify tokens with the shared chassis code. Identity already holds its public key, so
        # it uses it directly instead of fetching its own JWKS over HTTP like other services.
        init_auth(
            StaticKeyProvider({key.kid: key.public_key}),
            issuer=settings.jwt_issuer,
            audience=settings.jwt_audience,
        )
        init_db(settings.database_url)
        init_redis(settings.redis_url)
        # Identity can't do its job without either: login needs Postgres, and the denylist and
        # login rate limiting (security controls) need Redis
        register_readiness_check("database", db_ping_check)
        register_readiness_check("redis", redis_ping_check)
        # Publishes UserRegistered events from the outbox. Kafka is NOT a readiness check: while
        # Kafka is down, registration still works and events wait in the outbox
        tasks = [start_outbox_relay(settings)] if settings.kafka_enabled else []
        tasks += start_retention(maintenance.JOBS)  # E.14 clean-up, daily (Step 14.3b)
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
    app.include_router(jwks.router)
    app.include_router(auth.router)
    app.include_router(users.router)
    app.include_router(internal.router)
    return app
