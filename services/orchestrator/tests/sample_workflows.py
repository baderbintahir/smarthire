"""A minimal commit-first workflow for the orchestrator's own tests. Its own module because Temporal's
sandbox re-imports the module that defines a workflow, and test modules import pytest and friends."""

from temporalio import workflow

from app.workflows.base import COMMIT_RETRY, CommitFirst, call


@workflow.defn
class ThingWorkflow(CommitFirst):
    @workflow.run
    async def run(self, params: dict) -> str:
        if not await self.is_commit_accepted():
            return "REJECTED"
        await call("after_thing", "things", "POST", "/internal/after", {"id": params["id"]})
        return "DONE"

    @workflow.update
    async def commit(self, params: dict) -> dict:
        return await self.run_commit(
            lambda: call("make_thing", "things", "POST", "/internal/things", params, retry=COMMIT_RETRY))


@workflow.defn
class NotifyingWorkflow:
    """A parent that starts an email and finishes at once (the email must still be sent)."""

    @workflow.run
    async def run(self, params: dict) -> str:
        from app.workflows.email import send_email

        await send_email("JOB_READY", params["user_id"], {"job_title": "SRE"})
        return "PARENT_DONE"
