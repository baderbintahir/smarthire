# Import every model module here so Alembic autogenerate can see them
from app.models.candidate import Candidate, Resume  # noqa: F401
from app.models.messaging import OutboxEvent, ProcessedEvent  # noqa: F401
