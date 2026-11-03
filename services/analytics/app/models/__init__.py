# Import every model module here so Alembic autogenerate can see them
from app.models.messaging import ProcessedEvent  # noqa: F401
from app.models.projections import (  # noqa: F401
    AiUsageDaily,
    ApplicationsDaily,
    ApplicationTimeline,
    CandidateApplicationCount,
    FailuresDaily,
    JobApplicationCount,
    MetricCounter,
    PublishedJob,
)
