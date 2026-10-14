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
