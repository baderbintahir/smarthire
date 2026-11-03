# Import every model module here so Alembic autogenerate can see them
from app.models.messaging import ProcessedEvent  # noqa: F401
from app.models.notification import NotificationLog, NotificationTemplate  # noqa: F401
