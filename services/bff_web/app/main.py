"""bff_web: Backend for Frontend (Phase 12B, Appendix F.1). API composition for the web app's screens
that need several services at once. No database: it owns no data, it asks the owners.

Why here and not in the gateway: which fields, how to merge, what to do when a part fails are product
decisions (business logic). In a BFF they're ordinary, testable code; in the gateway they'd couple
infrastructure to every service's API and rule out an off-the-shelf gateway."""

from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from smarthire_common.auth import JWKSClient, KeyProvider, close_auth, init_auth
from smarthire_common.errors import register_exception_handlers
from smarthire_common.health import health_router, register_readiness_check
from smarthire_common.logging import setup_logging
from smarthire_common.metrics import setup_metrics
from smarthire_common.middleware import RequestIdMiddleware
from smarthire_common.redis import close_redis, init_redis, redis_ping_check
from smarthire_common.telemetry import flush_tracing, setup_tracing

from app.api import bff
from app.clients.downstream import CircuitBreaker, Downstream
from app.config import Settings

SERVICES = ("application", "candidate", "job", "scoring", "analytics")


def create_app(settings: Settings | None = None, *, key_provider: KeyProvider | None = None,
               transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    """transport lets tests replace the downstream services."""
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_redis(settings.redis_url)
        # The BFF verifies the token too (rule 7): a bad token is rejected before any fan-out
        init_auth(key_provider or JWKSClient(settings.jwks_url), issuer=settings.jwt_issuer,
                  audience=settings.jwt_audience)
        http = httpx.AsyncClient(transport=transport)
        app.state.services = {
            name: Downstream(name, getattr(settings, f"{name}_url"), http, timeout=settings.call_timeout_seconds,
                             breaker=CircuitBreaker(settings.breaker_failures, settings.breaker_reset_seconds))
            for name in SERVICES  # one breaker per service: Scoring being down doesn't block calls to Job
        }
        register_readiness_check("redis", redis_ping_check)  # downstream services are NOT readiness checks
        yield
        await http.aclose()
        await close_auth()
        await close_redis()
        flush_tracing()  # send spans still queued for Jaeger

    app = FastAPI(title=settings.service_name, lifespan=lifespan)
    setup_tracing(settings, app)  # spans for requests, SQL, Redis, outgoing HTTP (no-op unless enabled)
    app.state.settings = settings
    app.add_middleware(RequestIdMiddleware)
    register_exception_handlers(app)
    app.include_router(health_router)
    setup_metrics(app)  # GET /metrics + request count/latency
    app.include_router(bff.router)
    return app
