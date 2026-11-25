"""Application's event handlers (Step 10.4). Each runs inside the consumer's transaction, right after its
event id was recorded in processed_events (see smarthire_common.kafka). The job replica handlers of
Steps 8.2 and 10.5 became internal endpoints (app/api/internal.py, design v2.1)."""

from smarthire_common.events import EventEnvelope
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import workflows

CONSUMER_NAME = "application"  # the consumer group
# Our own topic only, until Step T9 (to start workflows). The job replica is kept current by the
# orchestrator's job workflows now (/internal/job-replicas/*, design v2.1), not by Job's events
TOPICS = ["applications.lifecycle"]


def build_handlers(settings) -> dict:
    async def on_application_submitted(session: AsyncSession, event: EventEnvelope) -> None:
        """Step 10.4: our own event starts the application's workflow (workflow id app-{id})."""
        await workflows.start(settings, event.payload["application_id"])

    return {"ApplicationSubmitted": on_application_submitted}
