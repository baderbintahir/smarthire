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
    # Normally one action = one workflow, ever: a retry after it finished gets its stored answer. Role
    # changes may legitimately run again later (grant, revoke, grant the same role)
    rerunnable: bool = False
    # For a finished action: is its stored answer THIS request's (same new id), or another request's for the
    # same natural key? Another's -> 409 with `duplicate` (code, message). None: every request is the same one
    same_request: Callable[[dict, dict], bool] | None = None
    duplicate: tuple[str, str] = ("CONFLICT", "This was already done")


ACTIONS: dict[str, Action] = {
    # Identity (W1, W2)
    "register": Action("RegistrationWorkflow", lambda k: f"register-{k['email_hash']}",
                       same_request=lambda i, r: r["id"] == i["user"]["id"],
                       duplicate=("EMAIL_ALREADY_REGISTERED", "This email is already registered")),
    "grant-role": Action("RoleChangeWorkflow", lambda k: f"role-{k['user_id']}-{k['role']}-grant", rerunnable=True),
    "revoke-role": Action("RoleChangeWorkflow", lambda k: f"role-{k['user_id']}-{k['role']}-revoke", rerunnable=True),
    # Candidate (W9)
    "upload-resume": Action("ResumeProcessingWorkflow", lambda k: f"resume-{k['resume_id']}"),
    # Job (W5, W6, W7): publishing and editing a READY job share the workflow; the id holds the version the
    # client saw, so a double click attaches and a later re-publish (newer version) is a new workflow
    "publish-job": Action("JobPublishingWorkflow", lambda k: f"publish-{k['job_id']}-v{k['version']}"),
    "edit-ready-job": Action("JobPublishingWorkflow", lambda k: f"publish-{k['job_id']}-v{k['version']}"),
    "close-job": Action("JobClosingWorkflow", lambda k: f"close-{k['job_id']}"),
    # Application (W10-W13). One application per candidate and job: a second apply attaches to the first
    "apply": Action("ApplicationWorkflow", lambda k: f"application-{k['job_id']}-{k['candidate_id']}",
                    same_request=lambda i, r: r["application"]["id"] == i["application"]["id"],
                    duplicate=("ALREADY_APPLIED", "You have already applied to this job")),
    "change-stage": Action("ApplicationActionWorkflow", lambda k: f"stage-{k['application_id']}-v{k['version']}"),
    "decide": Action("ApplicationActionWorkflow", lambda k: f"decision-{k['application_id']}"),
    "withdraw": Action("ApplicationActionWorkflow", lambda k: f"withdraw-{k['application_id']}"),
}
