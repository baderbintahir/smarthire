"""The one activity every workflow step uses: an HTTP call to the internal endpoint of the service that
owns the data (design v2.1). The orchestrator never touches another service's database (rule 1);
business rules run inside that endpoint, in its transaction.

Answer -> what Temporal does:
    2xx                         the step's result
    4xx (except 408, 429)       a BUSINESS answer (duplicate, quota, wrong state): NOT retried; the
                                workflow decides (answer the caller, compensate, skip)
    5xx, 408, 429, no answer    retried with the step's retry policy (the service is down or busy)

Every call carries a fresh service token and an Idempotency-Key that stays the same across retries of
the same step (workflow id + run id + activity id; the run id because a workflow id can be used again
later, e.g. grant, revoke, grant the same role), so the endpoint can return its first answer again."""

from dataclasses import dataclass

import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

from smarthire_common.service_auth import ServiceTokenSigner

BUSINESS_ERROR = "BusinessError"  # ApplicationError.type of a 4xx answer
RETRYABLE_4XX = {408, 429}  # timeout / too many requests: try again later


@dataclass
class ServiceCall:
    service: str  # a key of Settings.service_urls(), e.g. "identity"
    method: str  # GET, POST, PUT, DELETE
    path: str  # e.g. "/internal/users"
    body: dict | None = None
    timeout_seconds: float = 30.0


class ServiceSteps:
    def __init__(self, urls: dict[str, str], signer: ServiceTokenSigner, http: httpx.AsyncClient) -> None:
        self._urls, self._signer, self._http = urls, signer, http

    @activity.defn(name="call_service")
    async def call_service(self, call: ServiceCall) -> dict:
        info = activity.info()
        key = f"{info.workflow_id}:{info.workflow_run_id}:{info.activity_id}"
        headers = {**self._signer.headers(), "Idempotency-Key": key}
        try:
            response = await self._http.request(call.method, self._urls[call.service] + call.path, json=call.body,
                                                headers=headers, timeout=call.timeout_seconds)
        except httpx.HTTPError as exc:
            # Raising lets Temporal retry the step after its backoff
            raise ApplicationError(f"{call.service} unreachable: {type(exc).__name__}", type="ServiceUnavailable") from None
        if response.is_success:
            return response.json() if response.content else {}
        error = _error_of(response)
        if 400 <= response.status_code < 500 and response.status_code not in RETRYABLE_4XX:
            raise ApplicationError(
                error.get("message", f"{call.service} refused the request"),
                {"status": response.status_code, "code": error.get("code", "BAD_REQUEST"),
                 "message": error.get("message", ""), "details": error.get("details")},
                type=BUSINESS_ERROR, non_retryable=True,
            )
        raise ApplicationError(f"{call.service} answered {response.status_code}", type="ServiceError")


def _error_of(response: httpx.Response) -> dict:
    try:
        error = response.json().get("error")
    except ValueError:
        return {}
    return error if isinstance(error, dict) else {}
