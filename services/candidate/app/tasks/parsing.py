"""Celery task: parse a resume in the background (Step 10.2).

Why Celery and not a Temporal activity? It's ONE step with no orchestration: no saga, no
compensation, no waiting on other services. "Run this later, retry if it fails" is exactly what a
task queue does, with less machinery."""

import uuid

from smarthire_common.celery import run_async

from app.services import parsing
from app.tasks.celery_app import celery_app
from app.tasks.names import PARSE_RESUME


@celery_app.task(
    name=PARSE_RESUME,
    # Temporary failures (MinIO or Postgres down) are retried with growing waits: ~1, 2, 4, 8, 16 s
    # (randomized so many tasks don't retry in lockstep). An unreadable FILE is not an exception here:
    # the use case records it as FAILED and returns
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
)
def parse_resume(resume_id: str) -> str:
    return run_async(parsing.parse_resume(uuid.UUID(resume_id)))
