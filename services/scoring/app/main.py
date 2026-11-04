from contextlib import asynccontextmanager

from fastapi import FastAPI
from smarthire_common.background import stop_tasks
from smarthire_common.errors import NotFoundError, register_exception_handlers
from smarthire_common.health import health_router, register_readiness_check
from smarthire_common.logging import setup_logging
from smarthire_common.metrics import setup_metrics
from smarthire_common.middleware import RequestIdMiddleware
from smarthire_common.mongo import close_mongo, init_mongo, mongo_ping_check
from smarthire_common.mongo_outbox import ensure_outbox_indexes, start_mongo_outbox_relay
from smarthire_common.telemetry import flush_tracing, setup_tracing
from smarthire_common.temporal import start_worker

from app.config import Settings
from app.repositories import peers, scores
from app.workflows.activities import TASK_QUEUE, score_application


def create_app(settings: Settings | None = None, *, peer_transport=None) -> FastAPI:
    """peer_transport lets tests replace the Candidate/Enrichment APIs."""
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_mongo(settings.mongo_url, settings.mongo_database)
        await scores.ensure_indexes()
        await ensure_outbox_indexes()
        peers.init_peers(settings.candidate_url, settings.enrichment_url, transport=peer_transport)
        register_readiness_check("mongo", mongo_ping_check)
        tasks = []
        if settings.temporal_enabled:
            tasks.append(start_worker(settings, task_queue=TASK_QUEUE, activities=[score_application]))
        if settings.kafka_enabled:
            tasks.append(start_mongo_outbox_relay(settings))  # CandidateScored -> Kafka
        yield
        await stop_tasks(*tasks)
        await peers.close_peers()
        await close_mongo()
        flush_tracing()  # send spans still queued for Jaeger

    app = FastAPI(title=settings.service_name, lifespan=lifespan)
    setup_tracing(settings, app)  # spans for requests, SQL, Redis, outgoing HTTP (no-op unless enabled)
    app.state.settings = settings
    app.add_middleware(RequestIdMiddleware)
    register_exception_handlers(app)
    app.include_router(health_router)
    setup_metrics(app)  # GET /metrics + request count/latency

    @app.get("/scores/{application_id}", tags=["scores"])
    async def get_score(application_id: str) -> dict:
        """Internal read API (not routed via the gateway): the score and its explanation."""
        doc = await scores.get(application_id)
        if doc is None:
            raise NotFoundError("No score for this application", code="SCORE_NOT_FOUND")
        return {"application_id": doc.pop("_id"), **doc}

    return app
