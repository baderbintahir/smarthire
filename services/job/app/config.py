from smarthire_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "job"
    port: int = 8002
    database_url: str  # e.g. postgresql+asyncpg://<svc>_user:<pw>@postgres:5432/smarthire
    db_schema: str = "job"  # the service's own Postgres schema (used by Alembic)
    redis_url: str = "redis://localhost:6379/0"  # token denylist (shared with the gateway)
