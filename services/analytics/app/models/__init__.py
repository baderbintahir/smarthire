# Import every model module here so Alembic autogenerate can see them
from app.models.facts import Fact, FactKind  # noqa: F401
from app.models.projections import (  # noqa: F401
    AiUsageDaily,
    AiUsageKind,
    ApplicationsDaily,
    ApplicationTimeline,
    CandidateApplicationCount,
    FailuresDaily,
    JobApplicationCount,
    MetricCounter,
    PublishedJob,
    UserRole,
)
