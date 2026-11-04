from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    """Settings every service has. Each service subclasses this and adds its own
    fields (e.g. database_url). Values come from environment variables
    (SERVICE_NAME, ENVIRONMENT, LOG_LEVEL) or a local .env file."""

    # extra="ignore": a shared .env may hold variables meant for other services
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str  # required: a service must say who it is
    environment: str = "local"  # local | test | production
    log_level: str = "INFO"

    # --- Token verification (smarthire_common.auth); only used by services that check JWTs ---
    jwks_url: str = "http://identity:8001/.well-known/jwks.json"
    jwt_issuer: str = "smarthire-identity"
    jwt_audience: str = "smarthire"

    # --- Events (Phase 7); only used by services that publish or consume ---
    kafka_bootstrap_servers: str = "kafka:19092"  # inside Compose; localhost:9092 from the host
    schema_registry_url: str = "http://schema-registry:8081"
    # Start the outbox relay / consumers with the app. Tests turn this off (no Kafka there)
    kafka_enabled: bool = True

    # --- Workflows (Phase 9); only used by services with Temporal workers or clients ---
    temporal_address: str = "temporal:7233"
    temporal_namespace: str = "default"
    temporal_enabled: bool = True  # start the Temporal worker with the app; tests turn it off
    temporal_metrics_bind: str = "0.0.0.0:9464"  # the SDK's Prometheus metrics (Phase 13; scraped inside Compose)

    # --- Background tasks (Phase 10); only used by services with Celery tasks ---
    celery_broker_url: str = "amqp://guest:guest@rabbitmq:5672//"

    # --- Tracing (Phase 13). Off by default so tests never try to reach Jaeger; Compose turns it on ---
    tracing_enabled: bool = False
    otlp_endpoint: str = "http://jaeger:4318/v1/traces"  # OTLP over HTTP (no grpcio dependency)
