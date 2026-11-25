"""Helpers for internal endpoints (design v2.1): the endpoints the orchestrator calls as workflow steps.

Contract every internal endpoint that changes data follows:
  - `dependencies=[Depends(require_orchestrator)]` (smarthire_common.service_auth)
  - an `Idempotency-Key` header (workflow id + activity id): Temporal may run a step again after a
    timeout, so the endpoint must give the same answer the second time (pre-made ids, conditional
    updates, ON CONFLICT DO NOTHING); it never applies the change twice
  - a business rule that fails -> raise an AppError with a 4xx status: the orchestrator does NOT retry
    it and hands the status and code back to the caller
  - anything else (DB down, a bug) -> 5xx: the orchestrator retries with backoff"""

from fastapi import Header


async def idempotency_key(
    # Not named "key": endpoints often have a {key} path parameter, and FastAPI would mix the two up
    idempotency_key_header: str = Header(..., alias="Idempotency-Key", min_length=1, max_length=300),
) -> str:
    return idempotency_key_header
