"""How a service asks the orchestrator to run an action (design v2.1, "Workflow-first writes").

    result = await orchestrator.run_action("register", key={"email_hash": h}, input={...})

The orchestrator starts (or attaches to) the action's workflow and answers with the result of its
first step, the commit. Every outcome maps to one answer for the caller:

    answer                                   raised here
    200 {"result": ...}                      returned
    4xx business error                       the same AppError (status, code, message, details)
    could not connect at all                 WorkflowsUnavailableError: nothing was started
    sent, but no answer / 5xx                StillProcessingError: it may have started"""

import logging

import httpx

from smarthire_common.errors import AppError, StillProcessingError, WorkflowsUnavailableError
from smarthire_common.logging import request_id_var

logger = logging.getLogger(__name__)

# The orchestrator waits up to 7 s for the commit and then answers 503 STILL_PROCESSING itself; 9 s here
# leaves room for that answer, and stays under the gateway's 10 s (else the client sees a 504 instead)
DEFAULT_TIMEOUT_SECONDS = 9.0


class OrchestratorClient:
    def __init__(self, base_url: str, *, timeout: float = DEFAULT_TIMEOUT_SECONDS,
                 http_client: httpx.AsyncClient | None = None) -> None:
        self._http = http_client or httpx.AsyncClient(base_url=base_url, timeout=timeout)
        self._owns_http = http_client is None

    async def run_action(self, action: str, *, key: dict, input: dict) -> dict:
        headers = {}
        if request_id := request_id_var.get():
            headers["X-Request-ID"] = request_id  # the orchestrator's logs show the same id
        try:
            response = await self._http.post(f"/actions/{action}", json={"key": key, "input": input}, headers=headers)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
            # The request never left: no workflow can have started
            logger.warning("orchestrator unreachable for action %s", action)
            raise WorkflowsUnavailableError() from None
        except httpx.HTTPError:
            # Sent, then the answer was lost (read timeout, connection reset): it may have started
            logger.warning("no answer from the orchestrator for action %s", action, exc_info=True)
            raise StillProcessingError() from None

        if response.status_code == 200:
            return response.json()["result"]
        error = _error_body(response)
        if response.status_code < 500 and error is not None:
            # A business answer from the commit step: pass it on unchanged
            raise AppError(error.get("message", "Request failed"), code=error.get("code"),
                           http_status=response.status_code, details=error.get("details"))
        if error is not None and error.get("code") == WorkflowsUnavailableError.code:
            raise WorkflowsUnavailableError()  # the orchestrator itself couldn't reach Temporal
        raise StillProcessingError()

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()


def _error_body(response: httpx.Response) -> dict | None:
    try:
        error = response.json().get("error")
    except ValueError:
        return None
    return error if isinstance(error, dict) else None
