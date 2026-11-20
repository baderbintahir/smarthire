# Import every model module here so Alembic autogenerate can see them
from app.models.user import Role, User, UserRole  # noqa: F401
from app.models.messaging import OutboxEvent  # noqa: F401
