"""Step 7.2: the envelope, the catalog, and the JSON Schemas (no Kafka, no database)."""

import json
import re
from pathlib import Path

import jsonschema
import pytest

from smarthire_common import event_catalog
from smarthire_common.events import EventEnvelope, new_event

EVENT_TYPES = sorted(event_catalog.EVENT_TOPICS)
CREATE_TOPICS = Path(__file__).resolve().parents[3] / "infra" / "kafka" / "create-topics.sh"


def test_every_event_type_has_a_schema_file_and_vice_versa():
    files = sorted(p.stem for p in event_catalog.SCHEMAS_DIR.glob("*.json"))
    assert files == EVENT_TYPES


def test_every_topic_is_created_by_kafka_init():
    # The broker doesn't auto-create topics: an event whose topic isn't in create-topics.sh can't be sent
    created = set(re.findall(r"^\s+([a-z]+\.[a-z]+)\s*$", CREATE_TOPICS.read_text(), re.MULTILINE))
    assert set(event_catalog.EVENT_TOPICS.values()) <= created


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_envelope_schema_is_valid_and_accepts_the_example(event_type):
    schema = event_catalog.envelope_schema(event_type)
    jsonschema.Draft7Validator.check_schema(schema)
    for example in event_catalog.payload_schema(event_type)["examples"]:
        event = new_event(event_type, producer="test", payload=example)
        jsonschema.validate(event.model_dump(mode="json"), schema)


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_payloads_are_closed_and_require_their_fields(event_type):
    schema = event_catalog.envelope_schema(event_type)
    example = event_catalog.payload_schema(event_type)["examples"][0]
    good = new_event(event_type, producer="test", payload=example).model_dump(mode="json")

    typo = {**good, "payload": {**example, "user_idd": "x"}}  # unknown field
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(typo, schema)

    missing = {**good, "payload": {k: v for k, v in example.items() if k != next(iter(example))}}
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(missing, schema)


def test_event_type_in_envelope_must_match_the_schema():
    schema = event_catalog.envelope_schema("JobClosed")
    payload = event_catalog.payload_schema("JobClosed")["examples"][0]
    wrong = new_event("JobClosed", producer="test", payload=payload).model_dump(mode="json") | {"event_type": "JobReady"}
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(wrong, schema)


def test_new_event_fills_in_the_envelope():
    event = new_event("UserRegistered", producer="identity", payload={"user_id": "u", "role": "CANDIDATE"})
    assert event.event_id.version == 7 and event.schema_version == 1 and event.trace_context == {}
    assert event.occurred_at.tzinfo is not None
    assert EventEnvelope.model_validate_json(event.model_dump_json()) == event  # JSON round trip


def test_unknown_event_type_fails_at_the_producer():
    with pytest.raises(ValueError, match="Unknown event type"):
        new_event("UserRegistred", producer="identity", payload={})


def test_subjects_and_schema_text_are_stable():
    assert event_catalog.subject_for("JobClosed") == "jobs.lifecycle-JobClosed"
    # Registration and producers must produce the very same text (the registry looks it up by text)
    assert event_catalog.schema_str("JobClosed") == event_catalog.schema_str("JobClosed")
    assert json.loads(event_catalog.schema_str("JobClosed"))["title"] == "JobClosed"
    assert "examples" not in json.loads(event_catalog.schema_str("JobClosed"))["properties"]["payload"]
