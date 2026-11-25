"""orchestrator (design v2.1): runs every workflow and is the only service that talks to Temporal.

Services ask it to run an action (POST /actions/{action}); it runs the action's workflow, and each step
is an HTTP call to the internal endpoint of the service that owns the data. It owns no data itself and
holds no business rules: it only decides what happens next, and makes sure it does happen."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from smarthire_common.background import stop_tasks
from smarthire_common.errors import register_exception_handlers
from smarthire_common.health import health_router, register_readiness_check
from smarthire_common.logging import setup_logging
from smarthire_common.metrics import setup_metrics
from smarthire_common.middleware import RequestIdMiddleware
from smarthire_common.service_auth import ServiceTokenSigner
from smarthire_common.telemetry import flush_tracing, setup_tracing
from smarthire_common.temporal import connect, start_worker

from app.actions import ACTIONS
from app.api import actions
from app.codec import encrypted_converter, load_payload_key
from app.config import Settings
from app.steps import ServiceSteps
from app.workflows import WORKFLOWS


def create_app(settings: Settings | None = None, *, signer: ServiceTokenSigner | None = None, temporal_client=None,
               action_table: dict | None = None) -> FastAPI:
    """Tests pass their own signer, a Temporal test client and actions; production loads the keys and
    connects (and runs the worker) in the background, so the app starts even while Temporal is down."""
    settings = settings or Settings()
    setup_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.signer = signer or ServiceTokenSigner.from_pem(Path(settings.signing_key_path))
        app.state.temporal = temporal_client
        http = httpx.AsyncClient()
        tasks = []
        if temporal_client is None and settings.temporal_enabled:
            converter = encrypted_converter(load_payload_key(settings.payload_key_path))

            async def connect_client() -> None:
                app.state.temporal = await connect(settings, data_converter=converter)  # retries until it works

            steps = ServiceSteps(settings.service_urls(), app.state.signer, http)
            tasks = [asyncio.create_task(connect_client(), name="temporal-connect"),
                     start_worker(settings, task_queue=settings.task_queue, workflows=WORKFLOWS,
                                  activities=[steps.call_service], data_converter=converter)]

        async def temporal_check() -> None:
            if app.state.temporal is None:
                raise RuntimeError("not connected to Temporal yet")

        register_readiness_check("temporal", temporal_check)
        yield
        await stop_tasks(*tasks)
        await http.aclose()
        flush_tracing()

    app = FastAPI(title=settings.service_name, lifespan=lifespan)
    setup_tracing(settings, app)  # spans for /actions and for every outgoing step call
    app.state.settings = settings
    app.state.actions = ACTIONS if action_table is None else action_table
    app.add_middleware(RequestIdMiddleware)
    register_exception_handlers(app)
    app.include_router(health_router)
    setup_metrics(app)
    app.include_router(actions.router)
    return app
