"""The event envelope (Step 7.2): the wrapper every event travels in. Only `payload` differs per
event type; its shape is the event's JSON Schema in schemas/events/ (see event_catalog.py)."""

import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import AwareDatetime, BaseModel, Field

from smarthire_common.event_catalog import EVENT_TOPICS, topic_for  # noqa: F401  (re-exported)
from smarthire_common.models import new_id


class EventEnvelope(BaseModel):
    # Unique per event. Consumers record it in processed_events to skip redeliveries (Step 7.5)
    event_id: uuid.UUID = Field(default_factory=new_id)
    event_type: str  # "UserRegistered": tells the consumer how to read the payload
    schema_version: int = 1  # bumped when the payload schema gets a new (compatible) version
    occurred_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    producer: str  # the service that said it, e.g. "identity"
    # W3C trace headers (traceparent, ...) so a trace can follow the event; filled in Phase 13
    trace_context: dict[str, str] = Field(default_factory=dict)
    payload: dict[str, Any]


def new_event(event_type: str, *, producer: str, payload: dict[str, Any]) -> EventEnvelope:
    """Build an event. Unknown event types fail here, at the producer, not in some consumer later."""
    topic_for(event_type)
    return EventEnvelope(event_type=event_type, producer=producer, payload=payload)
