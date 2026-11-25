"""Job's actions (design v2.0/2.1, flows W5, W6, W7). The publishing saga moved here from the Job service.

JobPublishingWorkflow   actions "publish-job", "edit-ready-job"   id publish-{job}-v{version the client saw}
    commit: start_processing (Job)  ->  replica PROCESSING  ->  build_structured_job (Enrichment)  ->
    mark_ready (Job)  ->  replica READY, fact JobReady, email JOB_READY
    on failure: delete_structured_job (compensation)  ->  mark_failed  ->  replica FAILED, fact, email
JobClosingWorkflow      action "close-job"   id close-{job}   (CLOSED is final: a job closes once)
    commit: close (Job)  ->  close_job_applications (Application)  ->  signal each closed application"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

from app.workflows.application import application_workflow_id
from app.workflows.base import COMMIT_RETRY, CommitFirst, business_error, call, record_fact
from app.workflows.email import send_email

# A publishing step: up to 5 attempts (1, 2, 4, 8 s apart) and at most a minute in total, so a missing
# enrichment service fails the publish after a minute instead of waiting forever (as in Step 9.4)
STEP_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=2.0,
                         maximum_interval=timedelta(seconds=10), maximum_attempts=5)
STEP_LIMIT = timedelta(seconds=60)
# Undoing must try harder: it is what keeps the data consistent
COMPENSATION_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=30))
STALE = "STALE_JOB_VERSION"


def _is_stale(err: BaseException) -> bool:
    cause = business_error(err)
    return cause is not None and cause.details and cause.details[0].get("code") == STALE


@workflow.defn
class JobPublishingWorkflow(CommitFirst):
    @workflow.run
    async def run(self, params: dict) -> str:
        if not await self.wait_for_commit():
            return "REJECTED"
        job = self.committed  # the snapshot at the PROCESSING version, plus description and skills
        job_id, version = job["job_id"], job["version"]
        replica = f"/internal/job-replicas/{job_id}"
        await call("sync_job_replica", "application", "PUT", replica, job)  # PROCESSING: nobody applies meanwhile

        stored = False
        step = "build_structured_job"
        try:
            await call(step, "enrichment", "POST", "/internal/structured-jobs",
                       {"job_id": job_id, "version": version, "description": job["description"],
                        "required_skills": job["required_skills"]}, retry=STEP_RETRY, give_up_after=STEP_LIMIT)
            stored = True
            step = "mark_ready"
            ready = await call(step, "job", "POST", f"/internal/jobs/{job_id}/ready", {"version": version},
                               retry=STEP_RETRY, give_up_after=STEP_LIMIT)
        except ActivityError as err:
            if stored:  # undo the completed steps, in reverse order
                await call("delete_structured_job", "enrichment", "DELETE",
                           f"/internal/structured-jobs/{job_id}/{version}",
                           retry=COMPENSATION_RETRY, give_up_after=timedelta(minutes=10))
            if _is_stale(err):
                return "SUPERSEDED"  # edited or re-published since: a newer run owns the job
            reason = f"{step} failed: {err.cause}"[:500] if err.cause else f"{step} failed"
            workflow.logger.warning("publishing job %s v%s failed: %s", job_id, version, reason)
            try:
                failed = await call("mark_failed", "job", "POST", f"/internal/jobs/{job_id}/failed",
                                    {"version": version}, retry=STEP_RETRY)
            except ActivityError as mark_err:
                if _is_stale(mark_err):
                    return "SUPERSEDED"
                raise
            await call("sync_job_replica", "application", "PUT", replica, failed)
            await record_fact("JobPublishFailed", {"job_id": job_id, "version": failed["version"]})
            await send_email("JOB_PUBLISH_FAILED", failed["recruiter_id"], {"reason": reason})
            return "FAILED"

        await call("sync_job_replica", "application", "PUT", replica, ready)  # READY: applications open
        await record_fact("JobReady", {"job_id": job_id, "title": ready["title"], "version": ready["version"]})
        await send_email("JOB_READY", ready["recruiter_id"], {"job_title": ready["title"]})
        return "READY"

    @workflow.update
    async def commit(self, params: dict) -> dict:
        body = {"expected_version": params["expected_version"]}
        if "content" in params:  # the edit of a READY job: saved by the same step
            body |= {"content": params["content"], "stages": params["stages"]}
        return await self.run_commit(lambda: call("request_publish", "job", "POST",
                                                  f"/internal/jobs/{params['job_id']}/processing", body,
                                                  retry=COMMIT_RETRY))


@workflow.defn
class JobClosingWorkflow(CommitFirst):
    @workflow.run
    async def run(self, params: dict) -> str:
        if not await self.wait_for_commit():
            return "REJECTED"
        job = self.committed
        closed = await call("close_job_applications", "application", "POST",
                            f"/internal/job-replicas/{job['job_id']}/close", {"version": job["version"]})
        for application in closed["closed"]:
            try:  # stop scoring for an application that can't go on (best effort: it may have finished)
                await workflow.get_external_workflow_handle(
                    application_workflow_id(job["job_id"], application["candidate_id"])).signal("job_closed")
            except Exception:
                workflow.logger.info("no running workflow for application %s", application["application_id"])
        return "DONE"

    @workflow.update
    async def commit(self, params: dict) -> dict:
        return await self.run_commit(lambda: call("close_job", "job", "POST", f"/internal/jobs/{params['job_id']}/close",
                                                  {"expected_version": params["expected_version"]},
                                                  retry=COMMIT_RETRY))
