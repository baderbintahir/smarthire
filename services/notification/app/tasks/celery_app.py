"""Notification's Celery app (Step 11.1). The worker container runs it WITH the beat scheduler (-B),
which triggers the sweeper every minute:

    celery -A app.tasks.celery_app worker -B -Q emails

(Exactly one beat may run, so the worker is not scaled while beat is embedded in it.)"""

from celery.signals import worker_process_init, worker_process_shutdown
from smarthire_common.celery import make_celery
from smarthire_common.db import init_db
from smarthire_common.logging import setup_logging
from smarthire_common.telemetry import flush_tracing, setup_tracing

from app.config import Settings
from app.tasks.names import QUEUE, REQUEUE_STALE

settings = Settings()
celery_app = make_celery("notification", settings.celery_broker_url, default_queue=QUEUE)
celery_app.conf.worker_hijack_root_logger = False  # keep our JSON logging
celery_app.conf.include = ["app.tasks.email"]
celery_app.conf.beat_schedule = {"requeue-stale-emails": {"task": REQUEUE_STALE, "schedule": 60.0}}


@worker_process_init.connect
def _init_worker_process(**_kwargs) -> None:
    setup_logging(f"{settings.service_name}-worker", settings.log_level)
    # Tracing per CHILD process (its export thread must start after the fork). The Celery
    # instrumentation continues the trace of whoever enqueued the task (traceparent in the task headers)
    setup_tracing(settings, service_name=f"{settings.service_name}-worker")
    init_db(settings.database_url)


@worker_process_shutdown.connect
def _stop_worker_process(**_kwargs) -> None:
    flush_tracing()  # send the spans still queued
