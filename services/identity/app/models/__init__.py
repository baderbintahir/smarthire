# Import every model module here so Alembic autogenerate can see them
from app.models.user import Role, User, UserRole, UserStatus  # noqa: F401
