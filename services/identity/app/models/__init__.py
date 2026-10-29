# Import every model module here so Alembic autogenerate can see them
from app.models.refresh_token import RefreshToken  # noqa: F401
from app.models.user import RecruiterProfile, User  # noqa: F401
from app.models.messaging import OutboxEvent  # noqa: F401
