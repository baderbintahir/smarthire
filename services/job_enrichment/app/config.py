from smarthire_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "job_enrichment"
    port: int = 8003
    mongo_url: str = "mongodb://localhost:27017"  # mongodb://mongo:27017 in Compose
    mongo_database: str = "enrichment"  # this service's own database (database per service)
