"""Step T1 spike: "commit first" with Update-with-Start, plus encrypted payloads. Not part of SmartHire;
run it to SEE the behaviour design v2.0 relies on (docs/architecture.html, "Workflow-first writes").

    # terminal 1: the worker (runs the workflow and the activities)
    uv run --no-project --with temporalio --with cryptography python examples/temporal_commit/commit_demo.py worker
    # terminal 2: place an order; the caller waits for the commit step only
    uv run --no-project --with temporalio --with cryptography python examples/temporal_commit/commit_demo.py start 42 book

Checks (Step T1 "Done when"):
  1. run the same `start 42 book` twice while the first is still running: same answer, "write_order ran" once
  2. `start 43 duplicate`: a business error with a code reaches the caller, the workflow ends with no effect
  3. `start 44 book --address localhost:7299` (nothing listens there): fails fast, as if Temporal were down
  4. stop the worker, `start 45 book`: gives up after 10 s; start the worker again and the order is written
  5. `temporal workflow show -w order-42` inside the temporal container: payloads are encrypted

The fake database is a JSON file, so you can watch writes happen: /tmp/commit_demo_db.json

What the spike found (all five checks pass on server 1.32 / SDK 1.35):
  - Workflow code runs in a sandbox that re-imports its module; compiled extensions (cryptography)
    must be imported with workflow.unsafe.imports_passed_through(), or kept out of workflow modules.
  - Limit the wait with rpc_timeout and catch WorkflowUpdateRPCTimeoutOrCancelledError
    (asyncio.wait_for leaves the RPC dangling and raises a different error).
  - Parallel callers: the update handler must check-and-set its "committing" flag before its first
    await; a wait_condition first let all three callers schedule the activity.
  - Changing workflow code under a running history fails it with a nondeterminism error (TMPRL1100):
    use workflow.patched() or deploy when the affected workflows have finished."""

import asyncio
import contextlib
import dataclasses
import json
import os
import sys
from datetime import timedelta
from pathlib import Path

from temporalio import activity, workflow

# Workflow code runs in a sandbox that re-imports this module; a compiled extension like cryptography's
# can't be loaded twice, so it is passed through from the normal import system (workflows never call it).
with workflow.unsafe.imports_passed_through():
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from temporalio.api.common.v1 import Payload
from temporalio.client import (
    Client,
    WithStartWorkflowOperation,
    WorkflowUpdateFailedError,
    WorkflowUpdateRPCTimeoutOrCancelledError,
)
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy
from temporalio.converter import PayloadCodec, default as default_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.service import RPCError
from temporalio.worker import Worker

TASK_QUEUE = "commit-demo-tq"
DB = Path("/tmp/commit_demo_db.json")
WAIT_SECONDS = 10  # how long the caller waits for the commit (the API's STILL_PROCESSING limit)
# Demo key only. SmartHire reads its key from secrets/ (gitignored); a fixed key here keeps the demo self-contained.
KEY = bytes.fromhex(os.environ.get("DEMO_KEY", "00" * 32))


class EncryptionCodec(PayloadCodec):
    """Runs in the CLIENT and the WORKER, never in the Temporal server: the server (and its UI) only
    ever sees ciphertext. Each payload is serialized whole (metadata + data), then AES-GCM encrypted."""

    ENCODING = b"binary/encrypted"

    async def encode(self, payloads):
        out = []
        for p in payloads:
            nonce = os.urandom(12)  # AES-GCM needs a unique nonce per message
            data = nonce + AESGCM(KEY).encrypt(nonce, p.SerializeToString(), None)
            out.append(Payload(metadata={"encoding": self.ENCODING}, data=data))
        return out

    async def decode(self, payloads):
        out = []
        for p in payloads:
            if p.metadata.get("encoding") != self.ENCODING:
                out.append(p)  # not ours (e.g. written before encryption existed): pass through
                continue
            plain = AESGCM(KEY).decrypt(p.data[:12], p.data[12:], None)  # wrong key -> InvalidTag
            out.append(Payload.FromString(plain))
        return out


async def connect(address: str) -> Client:
    converter = dataclasses.replace(default_converter(), payload_codec=EncryptionCodec())
    return await Client.connect(address, data_converter=converter)


# ---------- activities: the I/O ----------

@activity.defn
async def write_order(order_id: str, item: str) -> dict:
    """The COMMIT step: one 'transaction'. Idempotent: a retry finds its own earlier write."""
    db = json.loads(DB.read_text()) if DB.exists() else {}
    if order_id in db:
        return db[order_id]
    if item == "duplicate":
        # A BUSINESS error: retrying can't fix it. type = the API error code, details = the HTTP status.
        raise ApplicationError("item already ordered", {"status": 409}, type="ALREADY_ORDERED", non_retryable=True)
    db[order_id] = {"order_id": order_id, "item": item}
    DB.write_text(json.dumps(db, indent=2))
    print(f"write_order ran: order {order_id}", flush=True)
    return db[order_id]


@activity.defn
async def follow_up(order_id: str) -> None:
    print(f"follow_up ran: order {order_id}", flush=True)  # stands for emails, facts, other services


# ---------- workflow: the decisions ----------

@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self.result: dict | None = None
        self.error: ApplicationError | None = None
        self.committing = False

    @workflow.run
    async def run(self, order_id: str) -> str:
        # Nothing happens until the commit update has finished one way or the other.
        await workflow.wait_condition(lambda: self.result is not None or self.error is not None)
        if self.error is not None:
            return "REJECTED"  # business error: end with no effect
        await workflow.sleep(timedelta(seconds=20))  # keeps it running so check 1 can attach to it
        await workflow.execute_activity(follow_up, order_id, start_to_close_timeout=timedelta(seconds=10))
        return "DONE"

    @workflow.update
    async def commit(self, order_id: str, item: str) -> dict:
        # A second caller (same workflow id) gets the stored answer instead of a second write.
        # Check and set the flag with no await in between: handlers interleave only at an await, so
        # exactly one caller starts the activity (found in the spike: wait_condition first let 3 through).
        if self.committing or self.result is not None or self.error is not None:
            await workflow.wait_condition(lambda: self.result is not None or self.error is not None)
            if self.error is not None:
                raise self.error
            return self.result
        self.committing = True
        try:
            self.result = await workflow.execute_activity(
                write_order, args=[order_id, item],
                start_to_close_timeout=timedelta(seconds=5),
                # infrastructure errors retry (worker down, DB down); business errors are non_retryable
                retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=10)),
            )
            return self.result
        except ActivityError as err:
            cause = err.cause
            if not isinstance(cause, ApplicationError):
                raise
            # Raising ApplicationError from an update fails only the update (the caller sees it);
            # other exceptions would fail the workflow task instead.
            self.error = ApplicationError(cause.message, *cause.details, type=cause.type, non_retryable=True)
            raise self.error


# ---------- the two programs ----------

async def run_worker(address: str) -> None:
    client = await connect(address)
    worker = Worker(client, task_queue=TASK_QUEUE, workflows=[OrderWorkflow], activities=[write_order, follow_up])
    print(f"worker polling {TASK_QUEUE}; Ctrl+C to stop", flush=True)
    await worker.run()


async def start(address: str, order_id: str, item: str) -> None:
    try:
        client = await asyncio.wait_for(connect(address), timeout=3)
    except (RuntimeError, RPCError, asyncio.TimeoutError, OSError) as err:
        print(f"-> 503 TEMPORAL_UNAVAILABLE: nothing was written ({type(err).__name__})")
        return
    op = WithStartWorkflowOperation(
        OrderWorkflow.run, order_id, id=f"order-{order_id}", task_queue=TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,  # same id running -> attach to it
    )
    try:
        result = await client.execute_update_with_start_workflow(
            OrderWorkflow.commit, args=[order_id, item], start_workflow_operation=op,
            rpc_timeout=timedelta(seconds=WAIT_SECONDS),  # the SDK's own limit; wait_for would leave the RPC dangling
        )
        handle = await op.workflow_handle()
        print(f"-> 201 {result} (workflow {handle.id}, run {handle.result_run_id})")
    except WorkflowUpdateFailedError as err:
        cause = err.cause
        details = cause.details if isinstance(cause, ApplicationError) else ()
        print(f"-> business error: code {getattr(cause, 'type', '?')}, details {list(details)}, message {cause}")
    except WorkflowUpdateRPCTimeoutOrCancelledError:
        with contextlib.suppress(Exception):
            await op.workflow_handle()  # the SDK stores the same error here too; read it to avoid a warning
        print(f"-> 503 STILL_PROCESSING: no answer in {WAIT_SECONDS} s; the workflow exists and will commit later")
    except RPCError as err:
        print(f"-> 503 TEMPORAL_UNAVAILABLE ({err.status.name})")


def main() -> None:
    args = sys.argv[1:]
    address = "localhost:7233"
    if "--address" in args:
        i = args.index("--address")
        address = args[i + 1]
        del args[i:i + 2]
    if args[:1] == ["worker"]:
        asyncio.run(run_worker(address))
    elif args[:1] == ["start"] and len(args) == 3:
        asyncio.run(start(address, args[1], args[2]))
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
