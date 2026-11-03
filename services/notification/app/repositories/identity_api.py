"""Recipients' addresses from Identity's internal API (/internal/users/{id}/contact).

Why look them up instead of putting emails in events: Kafka keeps and replicates events, and replays
them; an address in there is hard to remove ("forget me") and is personal data spread across systems.
IDs in events, contact details from their owner (Identity), when needed. The cost: Notification
depends on Identity at queue time (if Identity is down, the consumer waits and retries)."""

import httpx


class IdentityClient:
    def __init__(self, base_url: str, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._http = httpx.AsyncClient(base_url=base_url, timeout=5.0, transport=transport)

    async def contact(self, user_id: str) -> dict | None:
        """{"email", "status", ...}, or None if the user doesn't exist. Other failures raise."""
        response = await self._http.get(f"/internal/users/{user_id}/contact")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    async def close(self) -> None:
        await self._http.aclose()
