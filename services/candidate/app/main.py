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
from smarthire_common.outbox import start_outbox_relay
from smarthire_common.redis import close_redis, init_redis, redis_ping_check
from smarthire_common.retention import start_retention
from smarthire_common.telemetry import flush_tracing, setup_tracing

from app import maintenance
from app.api import candidates, internal
from app.config import Settings
from app.events import handlers
from app.repositories.files import init_storage, storage_ping_check


def create_app(settings: Settings | None = None, *, key_provider: KeyProvider | None = None) -> FastAPI:
    """key_provider lets tests verify tokens with a test key instead of fetching Identity's JWKS."""
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db(settings.database_url)
        init_redis(settings.redis_url)  # the token denylist lives in Redis
        init_storage(
            settings.s3_endpoint,
            settings.s3_access_key,
            settings.s3_secret_key,
            bucket=settings.resumes_bucket,
            secure=settings.s3_secure,
            public_endpoint=settings.s3_public_endpoint,
        )
        # Verify tokens ourselves too (rule 7), with Identity's public keys from its JWKS endpoint
        init_auth(
            key_provider or JWKSClient(settings.jwks_url),
            issuer=settings.jwt_issuer,
            audience=settings.jwt_audience,
        )
        register_readiness_check("database", db_ping_check)
        # Without Redis we can't check revocation, and protected routes fail closed: not ready
        register_readiness_check("redis", redis_ping_check)
        register_readiness_check("storage", storage_ping_check)  # uploads need the bucket
        # Background work: publish our events (ResumeUploaded) and consume others' (UserRegistered).
        # Kafka is not a readiness check: the API works without it; events wait or arrive later
        tasks = []
        if settings.kafka_enabled:
            tasks = [
                start_outbox_relay(settings),
                start_consumer(settings, name=handlers.CONSUMER_NAME, topics=handlers.TOPICS,
                               handlers=handlers.build_handlers(settings)),
            ]
        tasks += start_retention(maintenance.JOBS)  # E.14 clean-up, daily (Step 14.3b)
        yield
        await stop_tasks(*tasks)
        await close_auth()
        await close_redis()
        await close_db()
        flush_tracing()  # send spans still queued for Jaeger

    app = FastAPI(title=settings.service_name, lifespan=lifespan)
    setup_tracing(settings, app)  # spans for requests, SQL, Redis, outgoing HTTP (no-op unless enabled)
    app.state.settings = settings  # routes read limits from here (e.g. max_resume_bytes)
    app.add_middleware(RequestIdMiddleware)
    register_exception_handlers(app)
    app.include_router(health_router)
    setup_metrics(app)  # GET /metrics + request count/latency
    app.include_router(candidates.router)
    app.include_router(internal.router)
    return app
