"""Composed screens (Step 12B.2). The BFF only FETCHES and SHAPES: no business rules, no data of its own.

Essential parts decide the page: if Application refuses (403/404) the BFF passes that answer on as is.
Optional parts degrade: if Scoring is down the page still loads, with "score": null and "score" listed
in "degraded", so the UI can show "score unavailable" instead of an error page."""

import asyncio

from smarthire_common.errors import AppError, ServiceUnavailableError

from app.clients.downstream import Downstream, Reply, Unavailable


class Passthrough(AppError):
    """A service's own error answer (403, 404, ...), returned to the caller unchanged."""

    def __init__(self, reply: Reply) -> None:
        error = reply.body.get("error", {})
        super().__init__(error.get("message", "Request refused"), code=error.get("code", "ERROR"), http_status=reply.status)


async def _essential(call) -> dict:
    try:
        reply = await call
    except Unavailable as exc:
        raise ServiceUnavailableError(f"Can't load this page right now ({exc})") from exc
    if reply.status != 200:
        raise Passthrough(reply)
    return reply.body


async def _optional(name: str, call, degraded: list[str]):
    """The body, or None + `name` in degraded when the service failed or refused."""
    try:
        reply = await call
    except Unavailable:
        degraded.append(name)
        return None
    if reply.status != 200:
        degraded.append(name)
        return None
    return reply.body


async def application_view(services: dict[str, Downstream], application_id: str, authorization: str) -> dict:
    # 1. Application FIRST: it says whether the caller may see this application at all, and which
    #    candidate it belongs to (the other calls need that id). Its 403/404 ends the request here.
    application = await _essential(
        services["application"].get(f"/applications/{application_id}", authorization=authorization))
    candidate_id = application["candidate_id"]

    # 2. The rest IN PARALLEL: the page waits for the slowest call, not for the sum of all of them
    degraded: list[str] = []
    candidate, score, resume = await asyncio.gather(
        _optional("candidate", services["candidate"].get(f"/candidates/{candidate_id}", authorization=authorization), degraded),
        # Scoring's API is internal (no user token): only called once Application allowed this caller
        _optional("score", services["scoring"].get(f"/scores/{application_id}"), degraded),
        _optional("resume_download", services["candidate"].get(f"/candidates/{candidate_id}/resume/link",
                                                                authorization=authorization), degraded),
    )
    return {"application": application, "candidate": candidate,
            "score": {k: score[k] for k in ("score", "breakdown", "scored_at") if k in score} if score else None,
            "resume_download": resume, "degraded": sorted(degraded)}


HEADLINE_METRICS = ("total_jobs_published", "total_applications", "application_to_interview_rate", "average_days_to_hire")


async def recruiter_dashboard(services: dict[str, Downstream], authorization: str) -> dict:
    jobs_call = services["job"].get("/jobs", authorization=authorization, params={"mine": "true", "limit": 20})
    degraded: list[str] = []
    jobs_page, metrics = await asyncio.gather(
        _essential(jobs_call),
        _optional("metrics", services["analytics"].get("/analytics/metrics", authorization=authorization), degraded),
    )
    jobs = jobs_page["items"]
    # One summary call per job, all at once (a fan-out); failures blank only that job's counts
    summary_failures: list[str] = []
    summaries = await asyncio.gather(*[
        _optional("application_counts", services["application"].get(f"/jobs/{job['id']}/applications/summary",
                                                                    authorization=authorization), summary_failures)
        for job in jobs
    ])
    if summary_failures:
        degraded.append("application_counts")
    return {
        "jobs": [{"id": job["id"], "title": job["title"], "status": job["status"],
                  "applications": summary["by_stage"] if summary else None,
                  "total_applications": summary["total"] if summary else None}
                 for job, summary in zip(jobs, summaries, strict=True)],
        "metrics": {k: metrics.get(k) for k in HEADLINE_METRICS} if metrics else None,
        "degraded": sorted(degraded),
    }
