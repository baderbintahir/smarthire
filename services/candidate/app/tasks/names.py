"""Task and queue names: the contract between whoever enqueues (the API process) and the worker.
Kept apart from celery_app.py so enqueuing doesn't import the worker's setup."""

QUEUE = "resumes"
PARSE_RESUME = "parse_resume"
