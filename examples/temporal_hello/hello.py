"""Step 9.2: the smallest useful Temporal program. One workflow, two activities, a retry policy,
a worker and a starter. Not part of SmartHire; run it to SEE how Temporal behaves.

    # terminal 1: the worker (runs workflow + activity code)
    uv run --no-project --with temporalio python examples/temporal_hello/hello.py worker
    # terminal 2: start one workflow and wait for its result
    uv run --no-project --with temporalio python examples/temporal_hello/hello.py start Jane

Then open http://localhost:8088 and click the workflow: every step is in its history.
Kill the worker (Ctrl+C) during the 10 s timer, start it again: the workflow continues where it
was, because its state is the history stored in the Temporal server, not the worker's memory.

ACTIVITIES do the real work (network, DB, files, randomness). They may fail and be retried.
The WORKFLOW only decides what happens next. It must be DETERMINISTIC: after a worker restart,
Temporal REPLAYS the history through the workflow code to rebuild its state, and the code must make
the same decisions again. So no network calls, no random(), no datetime.now() inside a workflow:
use activities for I/O, and workflow.now() / workflow.sleep() for time (they come from the history)."""

import asyncio
import random
import sys
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.worker import Worker

TASK_QUEUE = "hello-tq"


@activity.defn
async def compose_greeting(name: str) -> str:
    # A flaky "network call": fails half the time. The retry policy below hides that from the workflow
    if random.random() < 0.5:
        raise RuntimeError("simulated network error")
    return f"Hello, {name}!"


@activity.defn
async def shout(text: str) -> str:
    return text.upper()


@workflow.defn
class HelloWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        retry = RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=2.0, maximum_attempts=5)
        greeting = await workflow.execute_activity(
            compose_greeting, name, start_to_close_timeout=timedelta(seconds=5), retry_policy=retry
        )
        workflow.logger.info("got %s; sleeping 10 s (kill the worker now!)", greeting)
        await workflow.sleep(timedelta(seconds=10))  # a durable timer, stored in the server
        return await workflow.execute_activity(shout, greeting, start_to_close_timeout=timedelta(seconds=5))


async def main() -> None:
    client = await Client.connect("localhost:7233")
    if sys.argv[1] == "worker":
        worker = Worker(client, task_queue=TASK_QUEUE, workflows=[HelloWorkflow], activities=[compose_greeting, shout])
        print("worker running on", TASK_QUEUE, "(Ctrl+C to stop)")
        await worker.run()
    else:
        name = sys.argv[2] if len(sys.argv) > 2 else "world"
        result = await client.execute_workflow(HelloWorkflow.run, name, id=f"hello-{name}", task_queue=TASK_QUEUE)
        print("result:", result)


if __name__ == "__main__":
    asyncio.run(main())
