from contextlib import asynccontextmanager

from fastapi import FastAPI
from smarthire_common.auth import JWKSClient, KeyProvider, close_auth, init_auth
from smarthire_common.background import stop_tasks
from smarthire_common.db import close_db, init_db
from smarthire_common.errors import register_exception_handlers
from smarthire_common.health import db_ping_check, health_router, register_readiness_check
from smarthire_common.kafka import start_consumer
from smarthire_common.logging import setup_logging
from smarthire_common.middleware import RequestIdMiddleware
from smarthire_common.outbox import start_outbox_relay
from smarthire_common.redis import close_redis, init_redis, redis_ping_check
from smarthire_common.temporal import start_worker

from app.api import applications
from app.config import Settings
from app.events import handlers
from app.repositories.candidate_api import CandidateClient, HttpCandidateClient
from app.workflows import activities
from app.workflows.processing import APPLICATION_TQ, ApplicationWorkflow


def create_app(
    settings: Settings | None = None,
    *,
    key_provider: KeyProvider | None = None,
    candidate_client: CandidateClient | None = None,
) -> FastAPI:
    """key_provider and candidate_client let tests replace Identity's JWKS and the Candidate API."""
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db(settings.database_url)
        init_redis(settings.redis_url)  # token denylist + idempotency keys
        init_auth(
            key_provider or JWKSClient(settings.jwks_url),
            issuer=settings.jwt_issuer,
            audience=settings.jwt_audience,
        )
        app.state.candidates = candidate_client or HttpCandidateClient(settings.candidate_url)
        register_readiness_check("database", db_ping_check)
        register_readiness_check("redis", redis_ping_check)
        # Publish our events and keep job_replica fresh from Job's. Kafka (and Candidate) are not
        # readiness checks: everything except applying keeps working without them
        tasks = []
        if settings.kafka_enabled:
            tasks = [
                start_outbox_relay(settings),
                start_consumer(settings, name=handlers.CONSUMER_NAME, topics=handlers.TOPICS,
                               handlers=handlers.build_handlers(settings)),
            ]
        if settings.temporal_enabled:  # runs ApplicationWorkflow and our own activities (application-tq)
            tasks.append(start_worker(settings, task_queue=APPLICATION_TQ, workflows=[ApplicationWorkflow],
                                      activities=activities.ACTIVITIES))
        yield
        await stop_tasks(*tasks)
        await app.state.candidates.close()
        await close_auth()
        await close_redis()
        await close_db()

    app = FastAPI(title=settings.service_name, lifespan=lifespan)
    app.state.settings = settings
    app.add_middleware(RequestIdMiddleware)
    register_exception_handlers(app)
    app.include_router(health_router)
    app.include_router(applications.router)
    return app
