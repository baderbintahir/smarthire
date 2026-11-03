from smarthire_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "analytics"
    port: int = 8008
    database_url: str  # e.g. postgresql+asyncpg://<svc>_user:<pw>@postgres:5432/smarthire
    db_schema: str = "analytics"  # the service's own Postgres schema (used by Alembic)
    redis_url: str = "redis://localhost:6379/0"  # token denylist + metrics cache (E.13 metrics:{name})
    metrics_cache_seconds: int = 10  # hot metrics are recomputed at most every 10 s
