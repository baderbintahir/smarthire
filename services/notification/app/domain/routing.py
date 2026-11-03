"""Which event becomes which email, to whom (pure, no I/O). Events carry user IDs, never addresses:
the address is looked up from Identity when the email is queued (app/events/handlers.py)."""

from dataclasses import dataclass

from smarthire_common.events import EventEnvelope


@dataclass(frozen=True)
class Notification:
    template: str
    recipient_id: str  # a user id; Identity knows the address
    variables: dict


DECISIONS = {
    "HIRED": "Congratulations! You have been selected for {job_title}. The team will contact you about next steps.",
    "REJECTED": "Thank you for your interest in {job_title}. After careful consideration, the team has decided "
                "not to move forward with your application.",
}


def route(event: EventEnvelope) -> Notification | None:
    """None: no email for this event (other types on the same topic, like JobUpdated)."""
    p = event.payload
    job_title = p.get("job_title") or "the job"  # job_title is optional (added in schema v2)
    match event.event_type:
        case "ApplicationSubmitted":
            return Notification("APPLICATION_RECEIVED", p["candidate_id"], {"job_title": job_title})
        case "ApplicationStageChanged":
            return Notification("STAGE_CHANGED", p["candidate_id"], {"job_title": job_title, "to_stage": p["to_stage"]})
        case "ApplicationDecisionMade":
            message = DECISIONS[p["decision"]].format(job_title=job_title)
            return Notification("DECISION_MADE", p["candidate_id"], {"job_title": job_title, "decision_message": message})
        case "JobReady":
            return Notification("JOB_READY", p["recruiter_id"], {"job_title": p["title"]})
        case "JobPublishFailed":
            return Notification("JOB_PUBLISH_FAILED", p["recruiter_id"], {"reason": p["reason"]})
    return None
