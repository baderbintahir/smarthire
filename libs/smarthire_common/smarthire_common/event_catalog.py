"""Which events exist, which topic each one goes to, and their JSON Schemas (Step 7.2).

Standard library ONLY: the `schema-init` container imports this module with a bare Python to
register the schemas, without installing the chassis' dependencies.

Schemas live next to this file in schemas/events/<EventType>.json and describe the PAYLOAD only.
envelope_schema() wraps a payload schema in the envelope every event shares (see events.py),
so the registry checks the whole message."""

import json
from pathlib import Path

SCHEMAS_DIR = Path(__file__).parent / "schemas" / "events"

# Event type -> topic (Appendix A). Several types share a topic when their order matters
# relative to each other (all of one job's events, in order, on jobs.lifecycle).
EVENT_TOPICS: dict[str, str] = {
    "UserRegistered": "identity.users",
    "JobUpdated": "jobs.lifecycle",
    "JobReady": "jobs.lifecycle",
    "JobPublishFailed": "jobs.lifecycle",
    "JobClosed": "jobs.lifecycle",
    "ResumeUploaded": "candidates.profile",
    "ResumeParsed": "candidates.profile",
    "ApplicationSubmitted": "applications.lifecycle",
    "ApplicationStageChanged": "applications.lifecycle",
    "ApplicationDecisionMade": "applications.lifecycle",
    "CandidateScored": "scoring.results",
    "AssistantQueryCompleted": "assistant.usage",
}


def topic_for(event_type: str) -> str:
    try:
        return EVENT_TOPICS[event_type]
    except KeyError:
        raise ValueError(f"Unknown event type {event_type!r}: add it to EVENT_TOPICS and schemas/events") from None


def subject_for(event_type: str) -> str:
    """Schema Registry subject: <topic>-<EventType> ("TopicRecordNameStrategy"). The default
    strategy allows ONE schema per topic, which doesn't fit topics carrying several event types."""
    return f"{topic_for(event_type)}-{event_type}"


def payload_schema(event_type: str) -> dict:
    topic_for(event_type)  # unknown types fail here with a clear message
    return json.loads((SCHEMAS_DIR / f"{event_type}.json").read_text())


def envelope_schema(event_type: str) -> dict:
    """The full message schema for one event type: the shared envelope, with `payload` being
    this event's schema. `title` is the record name the serializer uses to pick the subject."""
    payload = payload_schema(event_type)
    payload.pop("examples", None)  # documentation/test data, not part of the contract
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": event_type,
        "type": "object",
        "additionalProperties": False,
        "required": ["event_id", "event_type", "schema_version", "occurred_at", "producer", "trace_context", "payload"],
        "properties": {
            "event_id": {"type": "string", "format": "uuid"},
            "event_type": {"const": event_type},
            "schema_version": {"type": "integer", "minimum": 1},
            "occurred_at": {"type": "string", "format": "date-time"},
            "producer": {"type": "string", "minLength": 1},
            "trace_context": {"type": "object", "additionalProperties": {"type": "string"}},
            "payload": payload,
        },
    }


def schema_str(event_type: str) -> str:
    """The exact text registered in Schema Registry. Producers look their schema up by this text,
    so the registration script and the producers must build it the same way: here, once."""
    return json.dumps(envelope_schema(event_type), sort_keys=True, separators=(",", ":"))
