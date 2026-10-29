"""Register every event schema in Schema Registry (Step 7.2). Standard library only.

Runs in the one-shot `schema-init` container on every `docker compose up` (idempotent: registering
a schema identical to the latest version changes nothing). From the host: `make schemas`.

Subject per event type: <topic>-<EventType>. The registry checks every NEW version against the
subject's compatibility level (BACKWARD) and answers 409 if it would break existing readers:
then this script fails, and nothing incompatible ever reaches the registry."""

import json
import os
import sys
import urllib.error
import urllib.request

from smarthire_common.event_catalog import EVENT_TOPICS, schema_str, subject_for

REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")


def post(path: str, body: dict) -> dict:
    request = urllib.request.Request(
        f"{REGISTRY}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def main() -> int:
    failed = False
    for event_type in sorted(EVENT_TOPICS):
        subject = subject_for(event_type)
        try:
            result = post(f"/subjects/{subject}/versions", {"schemaType": "JSON", "schema": schema_str(event_type)})
            print(f"ok       {subject}  (schema id {result['id']})")
        except urllib.error.HTTPError as err:
            failed = True
            # 409 = incompatible with the registered versions; 422 = invalid schema
            print(f"FAILED   {subject}  HTTP {err.code}: {err.read().decode()}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
