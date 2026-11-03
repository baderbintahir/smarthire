"""Calling one downstream service: timeout + circuit breaker + "what did it answer".

The BFF never decides permissions: it FORWARDS the caller's Authorization header, so every service
still checks role and ownership itself (rule 7). A 403/404 from a service is an answer, not a failure;
only "couldn't get an answer" (timeout, connection refused, 5xx, breaker open) counts as unavailable."""

import asyncio
import time
from dataclasses import dataclass

import httpx


class Unavailable(Exception):
    """The service didn't answer usefully (down, slow, erroring, or its breaker is open)."""


@dataclass
class Reply:
    status: int
    body: dict


class CircuitBreaker:
    """After `failures` failures in a row: OPEN, every call fails at once for `reset_seconds` (no waiting
    on a timeout per request while the service is down). Then one call is let through: success closes it."""

    def __init__(self, failures: int, reset_seconds: float) -> None:
        self.max_failures, self.reset_seconds = failures, reset_seconds
        self.failures, self.opened_at = 0, None

    def allow(self) -> bool:
        if self.opened_at is None:
            return True
        if time.monotonic() - self.opened_at >= self.reset_seconds:
            return True  # half-open: let this one call test the service
        return False

    def success(self) -> None:
        self.failures, self.opened_at = 0, None

    def failure(self) -> None:
        self.failures += 1
        if self.failures >= self.max_failures:
            self.opened_at = time.monotonic()


class Downstream:
    def __init__(self, name: str, base_url: str, http: httpx.AsyncClient, *, timeout: float, breaker: CircuitBreaker) -> None:
        self.name, self.base_url, self.http, self.timeout, self.breaker = name, base_url.rstrip("/"), http, timeout, breaker

    async def get(self, path: str, *, authorization: str | None = None, params: dict | None = None) -> Reply:
        if not self.breaker.allow():
            raise Unavailable(f"{self.name}: circuit open")
        headers = {"Authorization": authorization} if authorization else {}
        try:
            # A HARD deadline for the whole call. httpx's own timeout limits each phase (connect, each read)
            # separately, so a slow-but-steady answer could still take much longer than `timeout`
            response = await asyncio.wait_for(
                self.http.get(f"{self.base_url}{path}", headers=headers, params=params, timeout=self.timeout), self.timeout)
        except (httpx.HTTPError, TimeoutError) as exc:  # timeout, refused connection, ...
            self.breaker.failure()
            raise Unavailable(f"{self.name}: {type(exc).__name__}") from exc
        if response.status_code >= 500:
            self.breaker.failure()
            raise Unavailable(f"{self.name}: HTTP {response.status_code}")
        self.breaker.success()
        return Reply(response.status_code, response.json() if response.content else {})
