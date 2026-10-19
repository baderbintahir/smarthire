from contextlib import asynccontextmanager

from fastapi import FastAPI
from smarthire_common.auth import JWKSClient, KeyProvider, close_auth, init_auth
from smarthire_common.db import close_db, init_db
from smarthire_common.errors import register_exception_handlers
from smarthire_common.health import db_ping_check, health_router, register_readiness_check
from smarthire_common.logging import setup_logging
from smarthire_common.middleware import RequestIdMiddleware
from smarthire_common.redis import close_redis, init_redis, redis_ping_check

from app.api import candidates
from app.config import Settings
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
        yield
        await close_auth()
        await close_redis()
        await close_db()

    app = FastAPI(title=settings.service_name, lifespan=lifespan)
    app.state.settings = settings  # routes read limits from here (e.g. max_resume_bytes)
    app.add_middleware(RequestIdMiddleware)
    register_exception_handlers(app)
    app.include_router(health_router)
    app.include_router(candidates.router)
    return app
