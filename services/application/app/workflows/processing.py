"""ApplicationWorkflow (Step 10.4): takes a new application from SUBMITTED to SCREENING with a score.

  init_stages (local) -> score_application (scoring-tq, Scoring's worker) -> record_score_and_advance

Scoring may take a while (the resume is parsed in the background, Scoring may be down), so it gets
generous retries and an overall deadline; past it, the score is PENDING_MANUAL and the application
still moves on (a recruiter scores it by hand). Signals `withdraw` and `job_closed` stop the workflow
early: nothing is recorded after the candidate left or the job closed.

Deterministic, like every workflow: decisions only, I/O happens in activities. Activities are called
by name, so this file imports no other service's code."""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

APPLICATION_TQ = "application-tq"
SCORING_TQ = "scoring-tq"
CANNOT_SCORE = "CannotScore"  # Scoring's non-retryable error type (no resume, unreadable resume...)

# Waiting for the resume to be parsed is normal: retry every few seconds, then up to a minute apart,
# for at most 10 minutes in total (including time waiting for a Scoring worker)
SCORING = {
    "start_to_close_timeout": timedelta(seconds=30),
    "schedule_to_close_timeout": timedelta(minutes=10),
    "retry_policy": RetryPolicy(initial_interval=timedelta(seconds=2), backoff_coefficient=2.0,
                                maximum_interval=timedelta(minutes=1), non_retryable_error_types=[CANNOT_SCORE]),
}
LOCAL = {"start_to_close_timeout": timedelta(seconds=10)}
RECORD = {"start_to_close_timeout": timedelta(seconds=20),
          "retry_policy": RetryPolicy(initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=30))}


@workflow.defn(name="ApplicationWorkflow")
class ApplicationWorkflow:
    def __init__(self) -> None:
        self.stop_reason: str | None = None

    @workflow.signal
    def withdraw(self) -> None:
        self.stop_reason = self.stop_reason or "WITHDRAWN"

    @workflow.signal
    def job_closed(self) -> None:
        self.stop_reason = self.stop_reason or "JOB_CLOSED"

    @workflow.run
    async def run(self, application_id: str) -> str:
        # A LOCAL activity: runs inside this worker, no round trip through a task queue (cheap DB read)
        context = await workflow.execute_local_activity("init_stages", args=[application_id], result_type=dict, **LOCAL)
        if not context["active"]:
            return "NOT_ACTIVE"

        scoring = workflow.start_activity(
            "score_application",
            args=[application_id, context["candidate_id"], context["job_id"], context["min_experience_years"]],
            task_queue=SCORING_TQ, result_type=dict, **SCORING)
        stopped = asyncio.ensure_future(workflow.wait_condition(lambda: self.stop_reason is not None))
        # Whichever comes first: the score, or a stop signal
        await workflow.wait([scoring, stopped], return_when=asyncio.FIRST_COMPLETED)
        if self.stop_reason is not None:
            scoring.cancel()
            return self.stop_reason
        stopped.cancel()

        failure = None
        try:
            result = scoring.result()
        except ActivityError as err:  # gave up (deadline) or impossible (no resume): a human scores it
            workflow.logger.warning("application %s not scored: %s", application_id, err.cause)
            result, failure = None, f"scoring gave up: {err.cause}"

        # `failure` becomes a SystemFailure event (Step 13.5), written with the result in one transaction
        await workflow.execute_activity("record_score_and_advance", args=[application_id, result, failure],
                                        task_queue=APPLICATION_TQ, **RECORD)
        return "SCREENING" if result is not None else "PENDING_MANUAL"
