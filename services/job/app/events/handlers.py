"""Job consumes its OWN command topic (Step 9.5): JobPublishRequested -> start the workflow.

Why go through Kafka instead of starting the workflow in the request? The request commits
PROCESSING + the outbox row atomically; this consumer then starts the workflow, retrying until it
works. Starting it twice is harmless: the workflow id publish-{job_id}-{version} is fixed, and
Temporal rejects a second start with the same id (REJECT_DUPLICATE), which we ignore."""

import logging

from smarthire_common.events import EventEnvelope
from smarthire_common.kafka import RetryLater
from smarthire_common.temporal import connect
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError

from app.workflows.publishing import JOB_TQ

log = logging.getLogger(__name__)

CONSUMER_NAME = "job-publisher"
TOPICS = ["jobs.commands"]


def workflow_id(job_id: str, version: int) -> str:
    return f"publish-{job_id}-{version}"  # one workflow per job VERSION: findable in the Temporal UI


def build_handlers(settings) -> dict:
    async def on_publish_requested(session: AsyncSession, event: EventEnvelope) -> None:
        job_id, version = event.payload["job_id"], event.payload["version"]
        client = await connect(settings)  # waits (retrying) while Temporal is down
        try:
            await client.start_workflow(
                "JobPublishingWorkflow", args=[job_id, version], id=workflow_id(job_id, version), task_queue=JOB_TQ,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,  # never run the same version twice
            )
            log.info("started %s", workflow_id(job_id, version))
        except WorkflowAlreadyStartedError:
            log.info("%s already started: ignoring the duplicate request", workflow_id(job_id, version))
        except RPCError as exc:
            raise RetryLater(f"Temporal unavailable: {exc}") from exc  # wait and retry; never dead-letter

    return {"JobPublishRequested": on_publish_requested}
