"""Application's actions (design v2.0/2.1, flows W10-W13). The application workflow moved here from the
Application service.

ApplicationWorkflow        action "apply"   id application-{job}-{candidate}
    commit: submit (Application)  ->  email APPLICATION_RECEIVED, fact ApplicationSubmitted  ->
    score_application (Scoring), unless a withdraw / job_closed signal comes first  ->
    record_score_and_advance (Application)  ->  facts CandidateScored, ApplicationStageChanged, email
ApplicationActionWorkflow  actions "change-stage" (id stage-{app}-v{version}), "decide" (decision-{app}),
                           "withdraw" (withdraw-{app})
    commit: the action (Application)  ->  fact and email; withdraw signals the ApplicationWorkflow"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

from app.workflows.base import COMMIT_RETRY, CommitFirst, call, record_fact, rejected
from app.workflows.email import send_email

with workflow.unsafe.imports_passed_through():
    from app.steps import ServiceCall

# Waiting for the resume to be parsed is normal: retry every few seconds, then up to a minute apart, for
# at most 10 minutes in total; past it the score is PENDING_MANUAL and the application still moves on
SCORING_RETRY = RetryPolicy(initial_interval=timedelta(seconds=2), backoff_coefficient=2.0,
                            maximum_interval=timedelta(minutes=1))
SCORING_LIMIT = timedelta(minutes=10)

DECISIONS = {  # the decision email's text (Notification's template fills in {decision_message})
    "HIRED": "Congratulations! You have been selected for {job_title}. The team will contact you about next steps.",
    "REJECTED": "Thank you for your interest in {job_title}. After careful consideration, the team has decided "
                "not to move forward with your application.",
}


def application_workflow_id(job_id: str, candidate_id: str) -> str:
    return f"application-{job_id}-{candidate_id}"


@workflow.defn
class ApplicationWorkflow(CommitFirst):
    def __init__(self) -> None:
        super().__init__()
        self.stop_reason: str | None = None

    @workflow.signal
    def withdraw(self) -> None:
        self.stop_reason = self.stop_reason or "WITHDRAWN"

    @workflow.signal
    def job_closed(self) -> None:
        self.stop_reason = self.stop_reason or "JOB_CLOSED"

    @workflow.run
    async def run(self, params: dict) -> str:
        if not await self.wait_for_commit():
            return "REJECTED"
        application, context = self.committed["application"], self.committed["context"]
        application_id, candidate_id, job_id = application["id"], application["candidate_id"], application["job_id"]
        title = context["job_title"]
        await send_email("APPLICATION_RECEIVED", candidate_id, {"job_title": title})
        await record_fact("ApplicationSubmitted", {"application_id": application_id, "job_id": job_id,
                                                   "candidate_id": candidate_id, "job_title": title})

        scoring = workflow.start_activity(
            "call_service", ServiceCall("scoring", "POST", "/internal/scores", {
                "application_id": application_id, "candidate_id": candidate_id, "job_id": job_id,
                "min_experience_years": context["min_experience_years"]}),
            start_to_close_timeout=timedelta(seconds=35), schedule_to_close_timeout=SCORING_LIMIT,
            retry_policy=SCORING_RETRY, summary="score_application", result_type=dict)
        stopped = asyncio.ensure_future(workflow.wait_condition(lambda: self.stop_reason is not None))
        # Whichever comes first: the score, or a stop signal (withdrawn, job closed)
        await workflow.wait([scoring, stopped], return_when=asyncio.FIRST_COMPLETED)
        if self.stop_reason is not None:
            scoring.cancel()
            return self.stop_reason
        stopped.cancel()

        try:
            result = scoring.result()
        except ActivityError as err:  # gave up (deadline) or impossible (422 CANNOT_SCORE): a human scores it
            workflow.logger.warning("application %s not scored: %s", application_id, err.cause)
            result = None
        recorded = await call("record_score_and_advance", "application", "POST",
                              f"/internal/applications/{application_id}/score",
                              {"score": result["score"] if result else None})
        if result:
            await record_fact("CandidateScored", {"application_id": application_id})
        if recorded["advanced"]:
            await record_fact("ApplicationStageChanged", {"application_id": application_id, "to_stage": "SCREENING"})
            await send_email("STAGE_CHANGED", candidate_id, {"job_title": title, "to_stage": "SCREENING"})
        if result is None:  # operators see it in Analytics' failure counts; the application still moved on
            await record_fact("SystemFailure", {"source": "application_workflow", "kind": "WORKFLOW_FAILED"})
            return "PENDING_MANUAL"
        return "SCREENING"

    @workflow.update
    async def commit(self, params: dict) -> dict:
        result = await self.run_commit(lambda: call("submit", "application", "POST", "/internal/applications",
                                                    params["application"], retry=COMMIT_RETRY))
        # The id is (job, candidate), so ANOTHER apply for the same job (another Idempotency-Key, so another
        # application id) can attach while this one runs: it is a duplicate, not a retry
        if result["application"]["id"] != params["application"]["id"]:
            raise rejected(409, "ALREADY_APPLIED", "You have already applied to this job")
        return result


@workflow.defn
class ApplicationActionWorkflow(CommitFirst):
    @workflow.run
    async def run(self, params: dict) -> str:
        if not await self.wait_for_commit():
            return "REJECTED"
        application, context = self.committed["application"], self.committed["context"]
        application_id, candidate_id = application["id"], application["candidate_id"]
        title = context["job_title"]
        if "to_stage" in params:
            await record_fact("ApplicationStageChanged", {"application_id": application_id, "to_stage": params["to_stage"]})
            await send_email("STAGE_CHANGED", candidate_id, {"job_title": title, "to_stage": params["to_stage"]})
        elif "decision" in params:
            await record_fact("ApplicationDecisionMade", {"application_id": application_id, "decision": params["decision"]})
            await send_email("DECISION_MADE", candidate_id, {
                "job_title": title, "decision_message": DECISIONS[params["decision"]].format(job_title=title)})
        else:  # withdrawn: stop the application's workflow if it is still scoring (best effort)
            try:
                await workflow.get_external_workflow_handle(
                    application_workflow_id(application["job_id"], candidate_id)).signal("withdraw")
            except Exception:
                workflow.logger.info("application %s: no running workflow to stop", application_id)
        return "DONE"

    @workflow.update
    async def commit(self, params: dict) -> dict:
        path = f"/internal/applications/{params['application_id']}"
        if "to_stage" in params:
            name, url, body = "change_stage", f"{path}/stage", {k: params[k] for k in ("to_stage", "version", "reason", "actor_id")}
        elif "decision" in params:
            name, url, body = "decide", f"{path}/decision", {k: params[k] for k in ("decision", "version", "reason", "actor_id")}
        else:
            name, url, body = "withdraw", f"{path}/withdraw", None
        return await self.run_commit(lambda: call(name, "application", "POST", url, body, retry=COMMIT_RETRY))
