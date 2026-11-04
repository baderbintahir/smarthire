"""Talking to running ApplicationWorkflows (start them, signal them)."""

import asyncio
import logging

from smarthire_common.kafka import RetryLater
from smarthire_common.metrics import WORKFLOWS_STARTED
from smarthire_common.temporal import connect
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError

from app.workflows.processing import APPLICATION_TQ

log = logging.getLogger(__name__)


def workflow_id(application_id: str) -> str:
    return f"app-{application_id}"  # one workflow per application: findable in the Temporal UI


async def start(settings, application_id: str) -> None:
    """Start once. A duplicate (redelivered event) is rejected by Temporal and ignored; Temporal being
    down makes the consumer retry later (never dead-letter)."""
    client = await connect(settings)
    try:
        await client.start_workflow("ApplicationWorkflow", application_id, id=workflow_id(application_id),
                                    task_queue=APPLICATION_TQ, id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE)
        WORKFLOWS_STARTED.labels("ApplicationWorkflow").inc()
        log.info("started %s", workflow_id(application_id))
    except WorkflowAlreadyStartedError:
        log.info("%s already started: ignoring the duplicate", workflow_id(application_id))
    except RPCError as exc:
        raise RetryLater(f"Temporal unavailable: {exc}") from exc


async def signal(settings, application_id: str, name: str, *, timeout: float = 3.0) -> None:
    """Best effort, AFTER the DB change committed: tell the workflow to stop early. If this fails (Temporal
    down, workflow already finished), nothing breaks: the workflow's last step only advances an
    application that is still ACTIVE, which a withdrawn or closed one isn't."""
    try:
        client = await asyncio.wait_for(connect(settings), timeout)
        await asyncio.wait_for(client.get_workflow_handle(workflow_id(application_id)).signal(name), timeout)
    except Exception as exc:  # noqa: BLE001  (finished workflow, Temporal down: both fine here)
        log.info("could not signal %s to %s: %s", name, workflow_id(application_id), exc)
