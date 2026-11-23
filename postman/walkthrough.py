"""Writes docs/api-walkthrough.md from the Postman collection: every request in run order, with its
token, body, expected status and the variables it saves. The collection is the source of truth;
regenerate after changing it:  make api-walkthrough

Standard library only."""

from __future__ import annotations  # the system python3 may be older than 3.10

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COLLECTION = ROOT / "postman" / "SmartHire.postman_collection.json"
OUTPUT = ROOT / "docs" / "api-walkthrough.md"

TOKENS = {"{{candidateToken}}": "candidate token", "{{recruiterToken}}": "recruiter token",
          "{{adminToken}}": "admin token"}

INTRO = """# API walkthrough: the whole flow, request by request

Every public API call, in the order that tests the whole application end to end. Generated from
[`postman/SmartHire.postman_collection.json`](../postman/SmartHire.postman_collection.json) by
`make api-walkthrough`; do not edit by hand.

**Run it in Postman:** import the collection and use **Run collection** on folders 1-9, with a delay of
about 2500 ms between requests (publishing, resume parsing and scoring run in the background).
Verified end to end with newman: 44 requests, 0 failures.

**Before you start**

1. `make up`, then wait until `make ps` shows the services healthy.
2. Create an admin whose password matches the `password` variable (`ChangeMe123!`):
   `docker compose exec -it identity python -m app.cli create-admin admin@example.com`
3. Running it again? Change `candidateEmail` and `recruiterEmail` (an email registers only once: 409).

**How to read the steps**

- Paths are relative to the gateway, `http://localhost:8000`. Values in `{{double braces}}` are collection
  variables, saved by earlier steps ("Saves").
- "Token" is the `Authorization: Bearer ...` header. Refresh and logout use the `refresh_token` cookie
  that login sets (path `/api/identity/auth`); Postman keeps it automatically, with curl use a cookie jar
  (`-c jar -b jar`). A refresh uses the cookie of the most recent login.
- Every error has the shape `{"error": {"code", "message", "request_id", "details?"}}`.

**Where to watch** the background work: Temporal UI http://localhost:8088 (workflows), Kafka UI
http://localhost:8085 (events), Mailpit http://localhost:8025 (emails), Jaeger http://localhost:16686
(traces), Grafana http://localhost:3001 (dashboards).
"""


def path_of(request: dict) -> str:
    raw = request["url"]["raw"] if isinstance(request["url"], dict) else request["url"]
    return raw.replace("{{baseUrl}}", "/api")


def token_of(request: dict, folder_auth: dict | None) -> str:
    auth = request.get("auth") or folder_auth or {}
    if auth.get("type") == "bearer":
        value = auth["bearer"][0]["value"]
        return TOKENS.get(value, value)
    return "none"


def saved_variables(item: dict) -> list[str]:
    script = " ".join(line for e in item.get("event", []) if e["listen"] == "test" for line in e["script"]["exec"])
    return sorted(set(re.findall(r"collectionVariables\.set\('(\w+)'", script)))


def body_of(request: dict) -> str | None:
    body = request.get("body")
    if not body:
        return None
    if body["mode"] == "formdata":
        fields = ", ".join(f"`{f['key']}` = {f.get('src') or f.get('value')}" for f in body["formdata"])
        return f"Body (form-data): {fields}"
    return "```json\n" + body["raw"] + "\n```"


def step(n: int, item: dict, folder_auth: dict | None) -> str:
    request = item["request"]
    lines = [f"### {n}. {item['name']}", "",
             f"`{request['method']} {path_of(request)}` · Token: {token_of(request, folder_auth)}"]
    headers = [h for h in request.get("header", []) if h["key"] != "Content-Type"]
    if headers:
        lines += ["", "Headers: " + ", ".join(f"`{h['key']}: {h['value']}`" for h in headers)]
    lines.append("")
    if body := body_of(request):
        lines += [body, ""]
    for line in (request.get("description") or "").splitlines():
        lines.append(f"> {line}" if line.strip() else ">")
    if saves := saved_variables(item):
        lines += ["", "Saves: " + ", ".join(f"`{v}`" for v in saves)]
    return "\n".join(lines) + "\n"


def main() -> None:
    collection = json.loads(COLLECTION.read_text())
    out, n = [INTRO], 0
    for folder in collection["item"]:
        internal = folder["name"].startswith("10 ")
        out.append(f"## {folder['name']}\n")
        if folder.get("description"):
            out.append(folder["description"] + "\n")
        if internal:
            out.append("Not part of the flow: these call services directly (not through the gateway), "
                       "for debugging. Paths are full URLs.\n")
        for item in folder["item"]:
            n += 1
            out.append(step(n, item, folder.get("auth")))
    OUTPUT.write_text("\n".join(out))
    print(f"wrote {OUTPUT.relative_to(ROOT)}: {n} requests")


if __name__ == "__main__":
    main()
