"""Every action a service can ask for: which workflow runs it and what its workflow id is.

The id is the action's NATURAL KEY, so two requests for the same thing meet in the same workflow
(conflict policy USE_EXISTING) and the commit runs once. Ids never contain personal data: they are
visible in Temporal UI and are not encrypted (registration uses a hash of the email)."""

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class Action:
    workflow: str  # the workflow type name
    workflow_id: Callable[[dict], str]  # from the request's "key"


ACTIONS: dict[str, Action] = {
    # Identity (W1, W2)
    "register": Action("RegistrationWorkflow", lambda k: f"register-{k['email_hash']}"),
    "grant-role": Action("RoleChangeWorkflow", lambda k: f"role-{k['user_id']}-{k['role']}-grant"),
    "revoke-role": Action("RoleChangeWorkflow", lambda k: f"role-{k['user_id']}-{k['role']}-revoke"),
    # Candidate (W9)
    "upload-resume": Action("ResumeProcessingWorkflow", lambda k: f"resume-{k['resume_id']}"),
    # Job (W5, W6, W7): publishing and editing a READY job share the workflow; the id holds the version the
    # client saw, so a double click attaches and a later re-publish (newer version) is a new workflow
    "publish-job": Action("JobPublishingWorkflow", lambda k: f"publish-{k['job_id']}-v{k['version']}"),
    "edit-ready-job": Action("JobPublishingWorkflow", lambda k: f"publish-{k['job_id']}-v{k['version']}"),
    "close-job": Action("JobClosingWorkflow", lambda k: f"close-{k['job_id']}"),
}
