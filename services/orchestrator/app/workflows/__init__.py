"""Every workflow the orchestrator runs. Filled flow by flow (Steps T5-T9)."""

from app.workflows.email import SendEmailWorkflow
from app.workflows.identity import RegistrationWorkflow, RoleChangeWorkflow

WORKFLOWS: list = [SendEmailWorkflow, RegistrationWorkflow, RoleChangeWorkflow]
