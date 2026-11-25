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
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import ApplicationError, WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from smarthire_common.errors import AppError, NotFoundError, StillProcessingError, WorkflowsUnavailableError
from smarthire_common.metrics import WORKFLOWS_STARTED

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
    workflow_id = spec.workflow_id(body.key)
    operation = WithStartWorkflowOperation(
        spec.workflow, body.input, id=workflow_id, task_queue=settings.task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,  # same action running -> attach to it
        # Finished -> don't run it again (see _answer_from_finished_workflow below), unless it failed or may rerun by design
        id_reuse_policy=(WorkflowIDReusePolicy.ALLOW_DUPLICATE if spec.rerunnable
                         else WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY),
    )
    try:
        result = await client.execute_update_with_start_workflow(
            "commit", body.input, start_workflow_operation=operation, result_type=dict,
            # The SDK's own limit: asyncio.wait_for would leave the RPC dangling (spike finding)
            rpc_timeout=timedelta(seconds=settings.commit_wait_seconds),
        )
    except WorkflowAlreadyStartedError:
        return await _answer_from_finished_workflow(client, workflow_id, spec, body.input)
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
        if err.status == RPCStatusCode.ALREADY_EXISTS:
            return await _answer_from_finished_workflow(client, workflow_id, spec, body.input)
        if err.status == RPCStatusCode.UNAVAILABLE:
            raise WorkflowsUnavailableError() from None
        log.warning("action %s: Temporal answered %s", action, err.status)
        raise StillProcessingError() from None
    WORKFLOWS_STARTED.labels(spec.workflow).inc()
    return {"result": result}


async def _answer_from_finished_workflow(client, workflow_id: str, spec, request_input: dict) -> dict:
    """The action already ran to the end: answer what its commit answered then (a retry after a lost
    answer), without running its follow-ups (emails, facts) a second time."""
    outcome = await client.get_workflow_handle(workflow_id).query("commit_outcome")
    if "error" in outcome:
        answer = outcome["error"]
        raise AppError(answer.get("message") or "Request refused", code=answer.get("code"),
                       http_status=answer.get("status", 409), details=answer.get("details"))
    if spec.same_request is not None and not spec.same_request(request_input, outcome["result"]):
        code, message = spec.duplicate
        raise AppError(message, code=code, http_status=409)
    log.info("%s already finished: answered from its history", workflow_id)
    return {"result": outcome["result"]}


@router.get("/.well-known/jwks.json", tags=["keys"])
async def jwks(request: Request) -> dict:
    """Public keys of the service tokens: every service verifies internal calls with them."""
    return request.app.state.signer.jwks()
