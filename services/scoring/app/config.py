from smarthire_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "scoring"
    port: int = 8006
    mongo_url: str = "mongodb://localhost:27017/?directConnection=true"  # mongo:27017 in Compose
    mongo_database: str = "scoring"  # this service's own database
    candidate_url: str = "http://candidate:8004"  # internal API: parsed resumes
    enrichment_url: str = "http://job_enrichment:8003"  # internal API: structured jobs
