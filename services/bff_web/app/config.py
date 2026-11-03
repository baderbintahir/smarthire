from smarthire_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "bff_web"
    port: int = 8011
    redis_url: str = "redis://localhost:6379/0"  # token denylist (the BFF verifies tokens too, rule 7)

    # The services it composes, called directly inside the network (not through the gateway)
    application_url: str = "http://application:8005"
    candidate_url: str = "http://candidate:8004"
    job_url: str = "http://job:8002"
    scoring_url: str = "http://scoring:8006"
    analytics_url: str = "http://analytics:8008"

    call_timeout_seconds: float = 2.0  # per call: one slow service can't hold the whole page for long
    breaker_failures: int = 3  # open a service's breaker after this many failures in a row...
    breaker_reset_seconds: float = 15.0  # ...and skip it for this long before trying again
