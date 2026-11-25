"""What every workflow shares (design v2.0 "Workflow-first writes", v2.1 orchestrator).

A commit-first workflow:
    - is started by POST /actions/{action} with Update-with-Start; the update is always `commit`
    - its `commit` update runs the commit step ONCE and keeps the answer; later callers attaching to
      the same workflow (double click, client retry) get that stored answer
    - its run() waits for the commit; a rejected commit (business error) ends the workflow with no effect

Workflow code must be deterministic: it is replayed from history after a restart. No I/O here, no
time.time(), no random: steps (activities) do the I/O. Changing a workflow while histories of it are
still running needs workflow.patched() (the spike hit TMPRL1100 without it)."""

from collections.abc import Awaitable, Callable
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():  # plain data + constants; httpx stays out of the sandbox
    from app.steps import BUSINESS_ERROR, ServiceCall

# The commit retries until it works: if the owning service is down the caller got 503 STILL_PROCESSING,
# and the change is made as soon as the service is back (never "half")
COMMIT_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=10))
# Follow-up steps: patient backoff, no attempt limit (a step's schedule_to_close can still cap it)
COMMIT_DEADLINE = timedelta(minutes=5)  # no commit by then: the request was lost
FOLLOW_UP_RETRY = RetryPolicy(initial_interval=timedelta(seconds=2), maximum_interval=timedelta(seconds=60))


async def call(step: str, service: str, method: str, path: str, body: dict | None = None, *,
               retry: RetryPolicy = FOLLOW_UP_RETRY, timeout_seconds: float = 30.0,
               give_up_after: timedelta | None = None) -> dict:
    """One workflow step = one internal endpoint call. `step` names it in Temporal UI's history."""
    return await workflow.execute_activity(
        "call_service", ServiceCall(service, method, path, body, timeout_seconds),
        start_to_close_timeout=timedelta(seconds=timeout_seconds + 5),
        schedule_to_close_timeout=give_up_after, retry_policy=retry, summary=step, result_type=dict,
    )


async def record_fact(kind: str, data: dict) -> None:
    """Hand Analytics a fact (design v2.0). The fact id is this workflow RUN + the kind, so a retried step
    is stored once, and a later run of the same workflow id (e.g. the same role granted again) counts again."""
    info = workflow.info()
    await call(f"record_fact {kind}", "analytics", "POST", "/internal/facts",
               {"fact_id": f"{info.workflow_id}:{info.run_id}:{kind}", "kind": kind,
                "occurred_at": workflow.now().isoformat(), "data": data})


def rejected(status: int, code: str, message: str, details=None) -> ApplicationError:
    """A business "no" decided by the workflow itself, answered to the caller like a service's 4xx."""
    return ApplicationError(message, {"status": status, "code": code, "message": message, "details": details},
                            type=BUSINESS_ERROR, non_retryable=True)


def business_error(err: BaseException) -> ApplicationError | None:
    """The 4xx answer behind a failed step, or None if the step failed for another reason."""
    cause = err.cause if isinstance(err, ActivityError) else err
    if isinstance(cause, ApplicationError) and cause.type == BUSINESS_ERROR:
        return cause
    return None


class CommitFirst:
    def __init__(self) -> None:
        self.committed: dict | None = None
        self.rejected: ApplicationError | None = None
        self._committing = False

    async def run_commit(self, step: Callable[[], Awaitable[dict]]) -> dict:
        """Call from the workflow's `commit` update handler."""
        # Check and set with no await in between: handlers interleave only at an await, so exactly one
        # caller runs the step (the spike's wait_condition-first version let three callers through)
        if self._committing or self.committed is not None or self.rejected is not None:
            await workflow.wait_condition(lambda: self.committed is not None or self.rejected is not None)
            if self.rejected is not None:
                raise self.rejected
            return self.committed
        self._committing = True
        try:
            self.committed = await step()
            return self.committed
        except ActivityError as err:
            cause = business_error(err)
            if cause is None:
                raise
            # Raising ApplicationError fails only the update (the caller gets it); anything else would
            # fail the workflow task and retry forever
            self.rejected = ApplicationError(cause.message, *cause.details, type=BUSINESS_ERROR, non_retryable=True)
            raise self.rejected

    async def is_commit_accepted(self) -> bool:
        """In run(): True once committed, False if the commit was rejected. If no commit arrives at all
        (the request was lost, e.g. the orchestrator restarted mid-request), the workflow FAILS after
        COMMIT_DEADLINE: a failed workflow's id may be used again, so the caller's retry starts afresh."""
        try:
            await workflow.wait_condition(lambda: self.committed is not None or self.rejected is not None,
                                          timeout=COMMIT_DEADLINE)
        except TimeoutError:
            raise ApplicationError("no commit arrived", type="NoCommit", non_retryable=True) from None
        return self.committed is not None

    @workflow.query
    def commit_outcome(self) -> dict:
        """The commit's answer, also after the workflow finished: a caller retrying a finished action
        gets this instead of a second run (which would repeat its follow-ups)."""
        if self.rejected is not None:
            return {"error": self.rejected.details[0] if self.rejected.details else {"status": 409}}
        return {"result": self.committed}
