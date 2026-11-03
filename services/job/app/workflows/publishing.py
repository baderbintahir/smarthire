"""JobPublishingWorkflow (Step 9.4): a saga that makes a job READY, or FAILED with clean-up.

  set_processing -> extract_skills -> breakdown_jd -> store_structured_job -> mark_ready
       (job-tq)       (enrichment-tq)  (enrichment-tq)    (enrichment-tq)       (job-tq)

Each step is an ACTIVITY run by whichever worker polls its task queue: Job's own worker for job-tq,
Job Enrichment's worker for enrichment-tq. Activities are called BY NAME, so this file imports no
other service's code (database per service holds for code, too).

If a step still fails after its retries, completed steps are undone in reverse order
(compensation: delete_structured_job), then mark_failed records FAILED + JobPublishFailed.

DETERMINISM: this code is replayed from its history after a worker restart, so it only DECIDES; it
never does I/O, randomness or wall-clock reads itself. Hence no imports from app (DB, config...)."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

JOB_TQ = "job-tq"
ENRICHMENT_TQ = "enrichment-tq"
STALE = "StaleJobVersion"  # the job was edited/re-published since this run started: nothing to do

# Every activity: up to 5 attempts, waiting 1 s, 2 s, 4 s, 8 s in between. And a hard cap of 60 s per
# step INCLUDING time waiting in the queue: if no enrichment worker is running, the step fails after a
# minute instead of waiting forever (retries only happen when a worker picks the task up and fails)
STEP = {
    "start_to_close_timeout": timedelta(seconds=20),
    "schedule_to_close_timeout": timedelta(seconds=60),
    "retry_policy": RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=2.0,
                                maximum_interval=timedelta(seconds=10), maximum_attempts=5,
                                non_retryable_error_types=[STALE]),
}
# Clean-up must try harder: undoing is what keeps data consistent
COMPENSATION = {
    "start_to_close_timeout": timedelta(seconds=20),
    "schedule_to_close_timeout": timedelta(minutes=10),
    "retry_policy": RetryPolicy(initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=30)),
}


def _is_stale(error: ActivityError) -> bool:
    return isinstance(error.cause, ApplicationError) and error.cause.type == STALE


@workflow.defn(name="JobPublishingWorkflow")
class JobPublishingWorkflow:
    @workflow.run
    async def run(self, job_id: str, version: int) -> str:
        try:
            job = await workflow.execute_activity("set_processing", args=[job_id, version], task_queue=JOB_TQ,
                                                  result_type=dict, **STEP)
        except ActivityError as err:
            if _is_stale(err):
                return "SUPERSEDED"  # a newer version has its own workflow
            raise

        compensations: list[tuple[str, list]] = []  # what to undo, in the order it was done
        try:
            extracted = await workflow.execute_activity(
                "extract_skills", args=[job_id, version, job["description"], job["required_skills"]],
                task_queue=ENRICHMENT_TQ, result_type=dict, **STEP)
            sections = await workflow.execute_activity("breakdown_jd", args=[job["description"]],
                                                       task_queue=ENRICHMENT_TQ, result_type=dict, **STEP)
            await workflow.execute_activity(
                "store_structured_job", args=[job_id, version, {**extracted, "sections": sections}],
                task_queue=ENRICHMENT_TQ, **STEP)
            compensations.append(("delete_structured_job", [job_id, version]))
            await workflow.execute_activity("mark_ready", args=[job_id, version], task_queue=JOB_TQ, **STEP)
            return "READY"
        except ActivityError as err:
            for name, args in reversed(compensations):  # undo in reverse order
                await workflow.execute_activity(name, args=args, task_queue=ENRICHMENT_TQ, **COMPENSATION)
            if _is_stale(err):
                return "SUPERSEDED"
            reason = f"{err.activity_type} failed: {err.cause}"[:500] if err.cause else str(err)[:500]
            workflow.logger.warning("publishing job %s v%s failed: %s", job_id, version, reason)
            await workflow.execute_activity("mark_failed", args=[job_id, version, reason], task_queue=JOB_TQ, **STEP)
            return "FAILED"
