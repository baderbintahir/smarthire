from contextlib import asynccontextmanager

from fastapi import FastAPI
from smarthire_common.db import close_db, init_db
from smarthire_common.errors import register_exception_handlers
from smarthire_common.health import db_ping_check, health_router, register_readiness_check
from smarthire_common.logging import setup_logging
from smarthire_common.middleware import RequestIdMiddleware
from smarthire_common.redis import close_redis, init_redis, redis_ping_check

from app.api import auth, jwks, users
from app.config import Settings
from app.domain.tokens import load_signing_key


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Load the key first: a missing/invalid key should stop startup, not fail every login
        app.state.signing_key = load_signing_key(settings.jwt_private_key_path)
        init_db(settings.database_url)
        init_redis(settings.redis_url)
        # Identity can't do its job without either: login needs Postgres, and the denylist and
        # login rate limiting (security controls) need Redis
        register_readiness_check("database", db_ping_check)
        register_readiness_check("redis", redis_ping_check)
        yield
        await close_redis()
        await close_db()

    app = FastAPI(title=settings.service_name, lifespan=lifespan)
    app.state.settings = settings
    app.add_middleware(RequestIdMiddleware)
    register_exception_handlers(app)
    app.include_router(health_router)
    app.include_router(jwks.router)
    app.include_router(auth.router)
    app.include_router(users.router)
    return app
