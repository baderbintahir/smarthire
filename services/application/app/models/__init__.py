# Import every model module here so Alembic autogenerate can see them
from app.models.application import (  # noqa: F401
    Application,
    ApplicationNote,
    ApplicationQuota,
    AuditEntry,
    JobReplica,
    StageHistory,
)
