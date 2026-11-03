"""The Candidate Service's Celery app (Step 10.1). The worker is a separate container from the API:

    celery -A app.tasks.celery_app worker -Q resumes

Celery runs plain functions; our DB and storage code is async, so each worker process gets its own
event loop (smarthire_common.celery.run_async) and its own DB engine, created after the fork."""

from celery.signals import worker_process_init
from smarthire_common.celery import make_celery
from smarthire_common.db import init_db
from smarthire_common.logging import setup_logging

from app.config import Settings
from app.repositories.files import init_storage
from app.tasks.names import QUEUE

settings = Settings()
celery_app = make_celery("candidate", settings.celery_broker_url, default_queue=QUEUE)
celery_app.conf.worker_hijack_root_logger = False  # keep our JSON logging
celery_app.conf.include = ["app.tasks.parsing"]


@worker_process_init.connect
def _init_worker_process(**_kwargs) -> None:
    """Runs in each worker child process: connections must not be shared across a fork."""
    setup_logging(f"{settings.service_name}-worker", settings.log_level)
    init_db(settings.database_url)
    init_storage(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key,
                 bucket=settings.resumes_bucket, secure=settings.s3_secure)
