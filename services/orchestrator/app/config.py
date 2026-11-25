from smarthire_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "orchestrator"
    port: int = 8012
    task_queue: str = "orchestrator-tq"  # every workflow and step runs on this one queue

    # Keys made by `make keys` (secrets/orchestrator/, gitignored, mounted read-only). Only this service has them
    signing_key_path: str = "/run/secrets/orchestrator/service_private.pem"  # signs service tokens
    payload_key_path: str = "/run/secrets/orchestrator/payload.key"  # AES-256 key, 64 hex characters

    # How long POST /actions waits for the commit step before answering 503 STILL_PROCESSING. Callers
    # (OrchestratorClient) wait 12 s, so this answer reaches them
    commit_wait_seconds: float = 10.0

    # The owners of the data: every workflow step is an HTTP call to one of these (inside the network)
    identity_url: str = "http://identity:8001"
    job_url: str = "http://job:8002"
    enrichment_url: str = "http://job_enrichment:8003"
    candidate_url: str = "http://candidate:8004"
    candidate_parser_url: str = "http://candidate-parser:8004"  # the Candidate image, for CPU-heavy parsing
    application_url: str = "http://application:8005"
    scoring_url: str = "http://scoring:8006"
    notification_url: str = "http://notification:8007"
    analytics_url: str = "http://analytics:8008"

    def service_urls(self) -> dict[str, str]:
        names = ("identity", "job", "enrichment", "candidate", "candidate_parser", "application", "scoring",
                 "notification", "analytics")
        return {name: getattr(self, f"{name}_url") for name in names}
