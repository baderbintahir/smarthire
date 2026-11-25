"""Every workflow the orchestrator runs. Filled flow by flow (Steps T5-T9)."""

from app.workflows.email import SendEmailWorkflow

WORKFLOWS: list = [SendEmailWorkflow]
