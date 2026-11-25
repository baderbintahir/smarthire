"""Every workflow the orchestrator runs. Filled flow by flow (Steps T5-T9)."""

from app.workflows.application import ApplicationActionWorkflow, ApplicationWorkflow
from app.workflows.candidate import ResumeProcessingWorkflow
from app.workflows.email import SendEmailWorkflow
from app.workflows.identity import RegistrationWorkflow, RoleChangeWorkflow
from app.workflows.job import JobClosingWorkflow, JobPublishingWorkflow

WORKFLOWS: list = [SendEmailWorkflow, RegistrationWorkflow, RoleChangeWorkflow, ResumeProcessingWorkflow,
                   JobPublishingWorkflow, JobClosingWorkflow, ApplicationWorkflow, ApplicationActionWorkflow]
