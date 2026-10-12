from smarthire_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "template"  # each real service overrides this default
    port: int = 8099  # each service uses its own port from the port map
    database_url: str  # e.g. postgresql+asyncpg://<svc>_user:<pw>@postgres:5432/smarthire
    db_schema: str = "template"  # the service's own Postgres schema (used by Alembic)
