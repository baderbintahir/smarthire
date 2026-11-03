from smarthire_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "notification"
    port: int = 8007
    database_url: str  # e.g. postgresql+asyncpg://<svc>_user:<pw>@postgres:5432/smarthire
    db_schema: str = "notification"  # the service's own Postgres schema (used by Alembic)
    identity_url: str = "http://identity:8001"  # internal API: recipients' email addresses
    smtp_host: str = "localhost"  # mailpit in Compose (catches every email, web UI on :8025)
    smtp_port: int = 1025
    smtp_from: str = "SmartHire <no-reply@smarthire.local>"
