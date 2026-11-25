"""POST /actions/{action}: how a service starts an action (design v2.1). Internal only: no gateway route.

Starts the action's workflow (or attaches to the running one with the same id) and waits for its
commit step, then answers:
    200 {"result": ...}           committed
    4xx (the step's own body)     a business rule said no; nothing was written
    503 WORKFLOWS_UNAVAILABLE     Temporal can't be reached; nothing was started
    503 STILL_PROCESSING          no answer within commit_wait_seconds; the workflow will finish"""

import contextlib
import logging
from datetime import timedelta

from fastapi import APIRouter, Request
from pydantic import BaseModel
from temporalio.client import (
    WithStartWorkflowOperation,
    WorkflowUpdateFailedError,
    WorkflowUpdateRPCTimeoutOrCancelledError,
)
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode

from smarthire_common.errors import AppError, NotFoundError, StillProcessingError, WorkflowsUnavailableError

from app.steps import BUSINESS_ERROR

log = logging.getLogger(__name__)
router = APIRouter(tags=["actions"])


class ActionRequest(BaseModel):
    key: dict  # the natural key: becomes the workflow id
    input: dict  # everything the workflow needs (passed to run() and to the commit update)


@router.post("/actions/{action}")
async def run_action(action: str, body: ActionRequest, request: Request) -> dict:
    spec = request.app.state.actions.get(action)
    if spec is None:
        raise NotFoundError(f"Unknown action {action}")
    client = request.app.state.temporal
    if client is None:  # still connecting at startup, or Temporal down since then
        raise WorkflowsUnavailableError()
    settings = request.app.state.settings
    operation = WithStartWorkflowOperation(
        spec.workflow, body.input, id=spec.workflow_id(body.key), task_queue=settings.task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,  # same action running -> attach to it
    )
    try:
        result = await client.execute_update_with_start_workflow(
            "commit", body.input, start_workflow_operation=operation, result_type=dict,
            # The SDK's own limit: asyncio.wait_for would leave the RPC dangling (spike finding)
            rpc_timeout=timedelta(seconds=settings.commit_wait_seconds),
        )
    except WorkflowUpdateFailedError as err:
        cause = err.cause
        if isinstance(cause, ApplicationError) and cause.type == BUSINESS_ERROR and cause.details:
            answer = cause.details[0]
            raise AppError(answer.get("message") or str(cause), code=answer.get("code"),
                           http_status=answer.get("status", 409), details=answer.get("details")) from None
        log.error("action %s: commit failed unexpectedly: %s", action, cause)
        raise StillProcessingError() from None
    except WorkflowUpdateRPCTimeoutOrCancelledError:
        with contextlib.suppress(Exception):
            await operation.workflow_handle()  # the SDK stores the same error there; read it (spike finding)
        raise StillProcessingError() from None
    except RPCError as err:
        with contextlib.suppress(Exception):
            await operation.workflow_handle()
        if err.status == RPCStatusCode.UNAVAILABLE:
            raise WorkflowsUnavailableError() from None
        log.warning("action %s: Temporal answered %s", action, err.status)
        raise StillProcessingError() from None
    return {"result": result}


@router.get("/.well-known/jwks.json", tags=["keys"])
async def jwks(request: Request) -> dict:
    """Public keys of the service tokens: every service verifies internal calls with them."""
    return request.app.state.signer.jwks()
