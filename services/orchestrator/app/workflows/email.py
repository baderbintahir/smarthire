"""SendEmailWorkflow (design v2.0, flow W14): one email = one workflow. It replaces the Celery send task,
its retries and the beat sweeper: Temporal itself remembers every email that isn't finished.

Started by other workflows as a CHILD with parent close policy ABANDON (fire and forget): the parent
doesn't wait for SMTP, and the email still gets its retries if the parent has already finished. Its id
is email-{parent workflow id}-{template}, which is also the notification_key in notification_log, so a
replayed parent can't send an email twice."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError
from temporalio.workflow import ParentClosePolicy

from app.workflows.base import business_error, call

# Same shape as the Celery task had: 5 s doubling to 5 min, 6 real attempts, then FAILED
SEND_RETRY = RetryPolicy(initial_interval=timedelta(seconds=5), backoff_coefficient=2.0,
                         maximum_interval=timedelta(minutes=5), maximum_attempts=6)


async def send_email(template: str, user_id: str, variables: dict) -> None:
    """For parent workflows: start the email and move on (not awaited beyond its start)."""
    await workflow.start_child_workflow(
        "SendEmailWorkflow", {"template": template, "user_id": user_id, "variables": variables},
        id=f"email-{workflow.info().workflow_id}-{template}",
        parent_close_policy=ParentClosePolicy.ABANDON,  # the email outlives the parent
    )


@workflow.defn
class SendEmailWorkflow:
    @workflow.run
    async def run(self, params: dict) -> str:
        # Events carried user ids, never addresses; the address is looked up when the email is sent
        try:
            contact = await call("get_contact", "identity", "GET", f"/internal/users/{params['user_id']}/contact")
        except ActivityError as err:
            if business_error(err) is not None:  # 404: the user doesn't exist (any more)
                return "NO_RECIPIENT"
            raise
        if contact["status"] != "ACTIVE":
            return "NO_RECIPIENT"  # suspended users get no email
        key = workflow.info().workflow_id
        await call("record_email", "notification", "POST", "/internal/emails",
                   {"notification_key": key, "recipient": contact["email"], "template_code": params["template"],
                    "variables": params["variables"]})
        try:
            result = await call("send_email", "notification", "POST", f"/internal/emails/{key}/send", retry=SEND_RETRY)
        except ActivityError:
            await call("mark_email_failed", "notification", "POST", f"/internal/emails/{key}/failed")
            return "FAILED"
        return result["status"]
