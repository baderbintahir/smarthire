from contextlib import asynccontextmanager

from fastapi import FastAPI
from smarthire_common.db import close_db, init_db
from smarthire_common.errors import register_exception_handlers
from smarthire_common.health import db_ping_check, health_router, register_readiness_check
from smarthire_common.logging import setup_logging
from smarthire_common.middleware import RequestIdMiddleware

from app.config import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup: runs once before the first request
        init_db(settings.database_url)
        register_readiness_check("database", db_ping_check)
        yield
        # Shutdown: runs once when the server stops
        await close_db()

    app = FastAPI(title=settings.service_name, lifespan=lifespan)
    app.add_middleware(RequestIdMiddleware)
    register_exception_handlers(app)
    app.include_router(health_router)
    # Real services add their routers here: app.include_router(users.router)
    return app
