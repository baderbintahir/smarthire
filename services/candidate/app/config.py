from smarthire_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "candidate"
    port: int = 8004
    database_url: str  # e.g. postgresql+asyncpg://<svc>_user:<pw>@postgres:5432/smarthire
    db_schema: str = "candidate"  # the service's own Postgres schema (used by Alembic)
    redis_url: str = "redis://localhost:6379/0"  # token denylist (shared with the gateway)

    # --- Object storage (MinIO, S3-compatible) for resume files ---
    s3_endpoint: str = "localhost:9000"  # host:port, no scheme (minio client style); minio:9000 in Compose
    s3_secure: bool = False  # TLS off locally; on in production
    s3_access_key: str  # required: the service's own MinIO user, not the root account
    s3_secret_key: str
    resumes_bucket: str = "resumes"
    max_resume_bytes: int = 5 * 1024 * 1024  # 5 MB, same limit as the CHECK in the resumes table
