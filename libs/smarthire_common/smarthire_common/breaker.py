"""A minimal circuit breaker for SYNCHRONOUS calls to another service (Phases 12B, 14.2).

After `failures` failures in a row the breaker is OPEN: every call fails at once for `reset_seconds`
instead of waiting for a timeout per request while the other service is down (which would tie up our
own workers and slow every caller). Then ONE call is let through ("half-open"): success closes it.

In-process state: each replica has its own breaker, which is fine (each learns quickly on its own)."""

import time


class CircuitOpen(Exception):
    """Raised (or reported) instead of calling a service whose breaker is open."""


class CircuitBreaker:
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
