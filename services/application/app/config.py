from smarthire_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "application"
    port: int = 8005
    database_url: str  # e.g. postgresql+asyncpg://<svc>_user:<pw>@postgres:5432/smarthire
    db_schema: str = "application"  # the service's own Postgres schema (used by Alembic)
    redis_url: str = "redis://localhost:6379/0"  # token denylist + idempotency keys

    # Candidate's API: the applicant's name and experience at apply time (with the user's own token)
    candidate_url: str = "http://candidate:8004"
    # How many applications one candidate may have open at once (SUBMITTED/SCREENING/IN_PROGRESS).
    # A global default for now; per-job limits are a later exercise in the plan
    max_active_applications: int = 10
    idempotency_ttl_seconds: int = 24 * 60 * 60  # E.13: idem:{user_id}:{key}, 24 h
