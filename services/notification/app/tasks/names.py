"""Task and queue names: the contract between whoever enqueues (the consumer) and the worker."""

QUEUE = "emails"
SEND_EMAIL = "send_email"
REQUEUE_STALE = "requeue_stale"
