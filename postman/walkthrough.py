"""Writes docs/api-walkthrough.md (read on GitHub) and docs/api-walkthrough.html (the docs site, next to
architecture.html) from the Postman collection: every request in run order, with its token, body,
expected status and the variables it saves. The collection is the source of truth; regenerate after
changing it:  make api-walkthrough

Standard library only."""

from __future__ import annotations  # the system python3 may be older than 3.10

import html
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COLLECTION = ROOT / "postman" / "SmartHire.postman_collection.json"
OUTPUT = ROOT / "docs" / "api-walkthrough.md"
OUTPUT_HTML = ROOT / "docs" / "api-walkthrough.html"

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

**Where to watch** the background work: Temporal UI http://localhost:8088 (workflows), Mailpit http://localhost:8025 (emails), Jaeger http://localhost:16686
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


INTERNAL_NOTE = ("Not part of the flow: these call services directly (not through the gateway), "
                 "for debugging. Paths are full URLs.")


def is_internal(folder: dict) -> bool:
    return folder["name"].startswith("10 ")


def write_markdown(collection: dict) -> int:
    out, n = [INTRO], 0
    for folder in collection["item"]:
        out.append(f"## {folder['name']}\n")
        if folder.get("description"):
            out.append(folder["description"] + "\n")
        if is_internal(folder):
            out.append(INTERNAL_NOTE + "\n")
        for item in folder["item"]:
            n += 1
            out.append(step(n, item, folder.get("auth")))
    OUTPUT.write_text("\n".join(out))
    return n


# --- HTML page: same look as architecture.html; layout, CSS and JS live in the template ---

TEMPLATE = Path(__file__).with_name("walkthrough.template.html")
TOKEN_CLASS = {"candidate token": "t-cand", "recruiter token": "t-rec", "admin token": "t-adm", "none": "t-none"}


def inline(text: str) -> str:
    """Escapes text, then renders the little markdown the collection uses: `code`, **bold**, {{variables}}."""
    out = html.escape(text, quote=False)
    out = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", out)
    out = re.sub(r"`([^`]+)`", r"<code>\1</code>", out)
    return variables(out)


def variables(escaped: str) -> str:
    return re.sub(r"\{\{(\w+)\}\}", r'<span class="var">{{\1}}</span>', escaped)


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def html_body(request: dict) -> str:
    body = request.get("body")
    if not body:
        return ""
    if body["mode"] == "formdata":
        fields = ", ".join(f"<code>{html.escape(f['key'])}</code> = {inline(str(f.get('src') or f.get('value')))}"
                           for f in body["formdata"])
        return f'<p class="fd">Body (form-data): {fields}</p>'
    raw = html.escape(body["raw"], quote=False)
    return (f'<div class="code"><button class="copy" type="button">Copy</button>'
            f"<pre><code>{variables(raw)}</code></pre></div>")


def html_description(text: str) -> str:
    paras = []
    for line in text.splitlines():
        if not line.strip():
            continue
        # "Role: ..." / "Success: ..." lines get their label in bold
        label = re.match(r"^(\w[\w ]{0,20}):\s", line)
        rest = inline(line[label.end():]) if label else inline(line)
        paras.append(f"<p><b>{html.escape(label.group(1))}:</b> {rest}</p>" if label else f"<p>{rest}</p>")
    return f'<div class="desc">{"".join(paras)}</div>' if paras else ""


def html_step(n: int, item: dict, folder_auth: dict | None) -> str:
    request = item["request"]
    method, token = request["method"], token_of(request, folder_auth)
    parts = [f'<article class="step" id="step-{n}">',
             f'<h3><span class="num">{n}</span>{html.escape(item["name"])}'
             f'<a class="anchor" href="#step-{n}" aria-label="Link to step {n}">#</a></h3>',
             f'<div class="req"><span class="m m-{method.lower()}">{method}</span>'
             f'<code class="path">{variables(html.escape(path_of(request)))}</code>'
             f'<span class="tok {TOKEN_CLASS.get(token, "t-none")}">{"no token" if token == "none" else html.escape(token)}</span></div>']
    headers = [h for h in request.get("header", []) if h["key"] != "Content-Type"]
    if headers:
        parts.append('<p class="hdrs">Headers: ' + ", ".join(
            f"<code>{html.escape(h['key'])}: {variables(html.escape(h['value']))}</code>" for h in headers) + "</p>")
    parts.append(html_body(request))
    parts.append(html_description(request.get("description") or ""))
    if saves := saved_variables(item):
        parts.append('<p class="saves">Saves ' + " ".join(f"<code>{v}</code>" for v in saves) + "</p>")
    parts.append("</article>")
    return "".join(parts)


def write_html(collection: dict) -> None:
    nav, sections, n = [], [], 0
    for folder in collection["item"]:
        fid = "f-" + slug(folder["name"])
        nav.append(f'<div class="ng" data-group="{fid}">{html.escape(folder["name"])}</div>')
        steps = []
        for item in folder["item"]:
            n += 1
            nav.append(f'<a href="#step-{n}" data-group="{fid}"><span class="n">{n}</span>{html.escape(item["name"])}</a>')
            steps.append(html_step(n, item, folder.get("auth")))
        intro = inline(folder["description"]) if folder.get("description") else ""
        if is_internal(folder):
            intro += ("<br>" if intro else "") + html.escape(INTERNAL_NOTE)
        sections.append(f'<section class="folder" id="{fid}"><h2>{html.escape(folder["name"])}</h2>'
                        + (f'<p class="note">{intro}</p>' if intro else "") + "".join(steps) + "</section>")
    page = TEMPLATE.read_text().replace("<!--NAV-->", "".join(nav)).replace("<!--STEPS-->", "\n".join(sections))
    OUTPUT_HTML.write_text(page)


def main() -> None:
    collection = json.loads(COLLECTION.read_text())
    n = write_markdown(collection)
    write_html(collection)
    print(f"wrote {OUTPUT.relative_to(ROOT)} and {OUTPUT_HTML.relative_to(ROOT)}: {n} requests")


if __name__ == "__main__":
    main()
