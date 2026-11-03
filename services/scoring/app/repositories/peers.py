"""Reading other services' INTERNAL APIs (Candidate, Job Enrichment). Scoring owns no copy of resumes
or jobs: it asks the owners at scoring time (Temporal retries the activity if one of them is down).

TODO(Phase 14): service-to-service authentication; today these calls rely on the internal network."""

import httpx

_http: httpx.AsyncClient | None = None
_urls: dict[str, str] = {}


def init_peers(candidate_url: str, enrichment_url: str, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
    """transport: tests pass a fake (httpx.MockTransport) instead of the network."""
    global _http
    _http = httpx.AsyncClient(timeout=5.0, transport=transport)
    _urls.update(candidate=candidate_url.rstrip("/"), enrichment=enrichment_url.rstrip("/"))


async def close_peers() -> None:
    global _http
    if _http is not None:
        await _http.aclose()
    _http = None


async def _get(url: str) -> dict | None:
    """The JSON body, or None for 404. Other failures raise (Temporal retries them)."""
    response = await _http.get(url)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


async def candidate_facts(candidate_id: str) -> dict | None:
    return await _get(f"{_urls['candidate']}/internal/candidates/{candidate_id}/resume")


async def structured_job(job_id: str) -> dict | None:
    return await _get(f"{_urls['enrichment']}/jobs/{job_id}/structured")
