from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from pydantic import BaseModel
from smarthire_common.auth import JWKSClient, KeyProvider
from smarthire_common.background import stop_tasks
from smarthire_common.errors import NotFoundError, register_exception_handlers
from smarthire_common.health import health_router, register_readiness_check
from smarthire_common.logging import setup_logging
from smarthire_common.metrics import setup_metrics
from smarthire_common.middleware import RequestIdMiddleware
from smarthire_common.mongo import close_mongo, init_mongo, mongo_ping_check
from smarthire_common.internal import idempotency_key
from smarthire_common.retention import start_retention
from smarthire_common.service_auth import close_service_auth, init_service_auth, require_orchestrator
from smarthire_common.telemetry import flush_tracing, setup_tracing

from app import maintenance
from app.config import Settings
from app.repositories import peers, scores
from app.services import scoring


class ScoreRequest(BaseModel):
    application_id: str
    candidate_id: str
    job_id: str
    min_experience_years: float | None = None


def create_app(settings: Settings | None = None, *, peer_transport=None,
               service_key_provider: KeyProvider | None = None) -> FastAPI:
    """peer_transport lets tests replace the Candidate/Enrichment APIs; service_key_provider the
    orchestrator's JWKS."""
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_mongo(settings.mongo_url, settings.mongo_database)
        await scores.ensure_indexes()
        peers.init_peers(settings.candidate_url, settings.enrichment_url, transport=peer_transport)
        register_readiness_check("mongo", mongo_ping_check)
        # Design v2.1: no Temporal worker, no outbox. The orchestrator's ApplicationWorkflow calls
        # POST /internal/scores and records CandidateScored as an analytics fact itself
        init_service_auth(service_key_provider or JWKSClient(settings.orchestrator_jwks_url))
        tasks = start_retention(maintenance.JOBS)  # E.14 clean-up, daily (Step 14.3b)
        yield
        await stop_tasks(*tasks)
        await close_service_auth()
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

    @app.post("/internal/scores", tags=["internal"], dependencies=[Depends(require_orchestrator),
                                                                   Depends(idempotency_key)])
    async def score_application(body: ScoreRequest) -> dict:
        """The score_application step (design v2.1): 422 CANNOT_SCORE is final, 503 means try again later."""
        return await scoring.score_application(body.application_id, body.candidate_id, body.job_id,
                                               body.min_experience_years)

    return app
