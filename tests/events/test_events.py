"""Events end to end against the RUNNING stack: `make up`, then `make test-events`.

Real Identity/Job/Candidate, real outbox relays, real Kafka + Schema Registry, real consumer. These
prove the wiring (topics, keys, schemas, consumer group); the logic itself is unit-tested in
libs/smarthire_common/tests and in each service."""

import json
import subprocess
import time
import uuid

import httpx
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

GATEWAY = "http://localhost:8000/api"
KAFKA = {"bootstrap.servers": "localhost:9092"}
REGISTRY = SchemaRegistryClient({"url": "http://localhost:8081"})


@pytest.fixture(scope="session", autouse=True)
def stack_running():
    try:
        httpx.get(f"{GATEWAY}/identity/.well-known/jwks.json", timeout=3).raise_for_status()
        httpx.get("http://localhost:8081/subjects", timeout=3).raise_for_status()
    except httpx.HTTPError:
        pytest.skip("Stack not running: `make up` first")


# ---------- helpers ----------


def register(role="CANDIDATE") -> tuple[str, str]:
    """Registers through the gateway; returns (user_id, access_token)."""
    email = f"ev-{uuid.uuid4().hex[:10]}@example.com"
    extra = {"display_name": "Rita"} if role == "RECRUITER" else {}
    r = httpx.post(f"{GATEWAY}/identity/auth/register", json={"email": email, "password": "correct-horse", "role": role, **extra})
    assert r.status_code == 201, r.text
    login = httpx.post(f"{GATEWAY}/identity/auth/login", json={"email": email, "password": "correct-horse"})
    return r.json()["id"], login.json()["access_token"]


def sql(query: str) -> str:
    """Runs SQL as the Postgres superuser inside the container (no passwords on the host)."""
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "sh", "-c", 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA'],
        input=query, capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def wait_for(check, timeout=20.0, what="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if result := check():
                return result
        except httpx.HTTPError:
            pass  # a service restarting: not ready yet
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


def read_messages(topic: str, key: str, expected: int = 1, timeout: float = 20.0) -> list:
    """All messages with this key, read from the start of the topic by a throwaway consumer group."""
    consumer = Consumer({**KAFKA, "group.id": f"test-{uuid.uuid4()}", "auto.offset.reset": "earliest"})
    consumer.subscribe([topic])
    found, deadline = [], time.time() + timeout
    try:
        while time.time() < deadline and len(found) < expected:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error() and msg.key() == key.encode():
                found.append(msg)
    finally:
        consumer.close()
    assert len(found) >= expected, f"found {len(found)} of {expected} messages with key {key} on {topic}"
    return found


def decode(msg) -> dict:
    return JSONDeserializer(None, schema_registry_client=REGISTRY)(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))


def candidate_logs() -> str:
    return subprocess.run(["docker", "compose", "logs", "candidate", "--since", "5m"], capture_output=True, text=True).stdout


# ---------- 7.6 UserRegistered -> candidate profile ----------


def test_registering_a_candidate_creates_their_profile():
    user_id, token = register()
    profile = wait_for(
        lambda: (r := httpx.get(f"{GATEWAY}/candidates/me", headers={"Authorization": f"Bearer {token}"})).status_code == 200 and r.json(),
        what="the candidate profile",
    )
    assert profile["id"] == user_id and profile["full_name"] == ""
    assert sql(f"SELECT published_at IS NOT NULL FROM identity.outbox WHERE message_key = '{user_id}'") == "t"


def test_event_in_kafka_is_keyed_by_user_and_matches_its_schema():
    user_id, _ = register()
    [msg] = read_messages("identity.users", user_id)
    assert msg.value()[0] == 0  # Confluent wire format: magic byte 0, then the 4-byte schema id
    event = decode(msg)  # validated against the registered schema
    assert event["event_type"] == "UserRegistered" and event["payload"] == {"user_id": user_id, "role": "CANDIDATE"}


def test_recruiters_get_no_candidate_profile():
    user_id, _ = register("RECRUITER")
    [msg] = read_messages("identity.users", user_id)
    event_id = decode(msg)["event_id"]
    wait_for(lambda: sql(f"SELECT count(*) FROM candidate.processed_events WHERE event_id = '{event_id}'") == "1",
             what="the candidate consumer to process the event")
    assert sql(f"SELECT count(*) FROM candidate.candidates WHERE id = '{user_id}'") == "0"


def test_replayed_event_is_skipped_by_the_consumer():
    user_id, _ = register()
    [msg] = read_messages("identity.users", user_id)
    event_id = decode(msg)["event_id"]
    wait_for(lambda: sql(f"SELECT count(*) FROM candidate.candidates WHERE id = '{user_id}'") == "1", what="the profile")

    # Deliver the very same bytes again, as Kafka would after a consumer crash
    producer = Producer(KAFKA)
    producer.produce("identity.users", key=msg.key(), value=msg.value())
    producer.flush(10)
    wait_for(lambda: f"skipped duplicate UserRegistered {event_id}" in candidate_logs(), what="the duplicate to be skipped")
    assert sql(f"SELECT count(*) FROM candidate.processed_events WHERE event_id = '{event_id}'") == "1"


def test_unreadable_message_goes_to_the_dead_letter_queue():
    """Every consumer GROUP on identity.users (Candidate, Analytics) parks its own copy, independently."""
    key = f"poison-{uuid.uuid4()}"
    producer = Producer(KAFKA)
    producer.produce("identity.users", key=key, value=b"this is not an event")
    producer.flush(10)
    dead = read_messages("identity.users.dlq", key, expected=2)
    assert all(m.value() == b"this is not an event" for m in dead)  # kept unchanged for a human to inspect
    headers = [dict(m.headers()) for m in dead]
    assert {h["x-consumer"] for h in headers} == {b"candidate", b"analytics"}
    assert all(h["x-original-topic"] == b"identity.users" and h["x-error"] for h in headers)


# ---------- 7.7 job events ----------


def test_job_updated_and_closed_reach_kafka_in_order():
    recruiter_id, token = register("RECRUITER")
    auth = {"Authorization": f"Bearer {token}"}
    body = {"title": "Backend Engineer", "description": "APIs", "employment_type": "FULL_TIME", "stages": ["Screening", "Offer"]}
    job = httpx.post(f"{GATEWAY}/jobs", json=body, headers=auth).json()
    assert httpx.put(f"{GATEWAY}/jobs/{job['id']}", json={**body, "title": "Senior Backend Engineer", "version": 1},
                     headers=auth).status_code == 200
    sql(f"UPDATE job.jobs SET status = 'READY' WHERE id = '{job['id']}'")  # stand-in for Phase 9's workflow
    assert httpx.post(f"{GATEWAY}/jobs/{job['id']}/close", headers=auth).status_code == 200

    events = [decode(m) for m in read_messages("jobs.lifecycle", job["id"], expected=2)]
    assert [e["event_type"] for e in events] == ["JobUpdated", "JobClosed"]  # same key: same partition, in order
    assert events[0]["payload"]["title"] == "Senior Backend Engineer"
    assert events[0]["payload"]["recruiter_id"] == recruiter_id
    assert events[0]["payload"]["version"] < events[1]["payload"]["version"]


# ---------- Phase 8: job replica + applying end to end ----------


def published_job(token: str, **overrides) -> dict:
    """Create and publish a job through the gateway, then wait for the REAL publishing saga
    (Temporal + Job Enrichment) to make it READY."""
    auth = {"Authorization": f"Bearer {token}"}
    body = {"title": "Data Engineer", "description": "You will build Kafka pipelines in Python.\nRequirements:\n- 2 years of SQL",
            "employment_type": "FULL_TIME", "stages": ["Screening", "Interview"], "required_skills": ["Python"], **overrides}
    job = httpx.post(f"{GATEWAY}/jobs", json=body, headers=auth).json()
    assert httpx.post(f"{GATEWAY}/jobs/{job['id']}/publish", headers=auth).status_code == 202
    return wait_for(lambda: (j := httpx.get(f"{GATEWAY}/jobs/{job['id']}", headers=auth).json())["status"] == "READY" and j,
                    timeout=60, what="the job to become READY")


# ---------- Phase 9: the publishing saga ----------


def test_publishing_runs_the_saga_to_ready():
    recruiter_id, token = register("RECRUITER")
    job = published_job(token)
    # Job Enrichment stored the structured document (internal API, not via the gateway)
    structured = httpx.get(f"http://localhost:8003/jobs/{job['id']}/structured").json()
    assert structured["skills"][:1] == ["python"] and "kafka" in structured["skills"]
    assert structured["sections"]["requirements"] == ["2 years of SQL"]
    # JobPublishRequested -> workflow -> JobReady, all keyed by the job
    assert decode(read_messages("jobs.commands", job["id"])[0])["event_type"] == "JobPublishRequested"
    ready = [decode(m) for m in read_messages("jobs.lifecycle", job["id"])]
    assert [e["event_type"] for e in ready] == ["JobReady"] and ready[0]["payload"]["recruiter_id"] == recruiter_id
    # ...and Application's replica followed
    wait_for(lambda: sql(f"SELECT status FROM application.job_replica WHERE job_id = '{job['id']}'") == "READY",
             what="the replica")


def test_candidate_applies_to_a_job_known_only_through_events():
    recruiter_id, recruiter_token = register("RECRUITER")
    rec = {"Authorization": f"Bearer {recruiter_token}"}
    job = published_job(recruiter_token, eligibility_rules={"min_experience_years": 2})  # the real saga
    wait_for(lambda: sql(f"SELECT status FROM application.job_replica WHERE job_id = '{job['id']}'") == "READY",
             what="the replica to become READY")

    # A candidate with a complete profile applies through the gateway
    candidate_id, token = register()
    auth = {"Authorization": f"Bearer {token}"}
    wait_for(lambda: httpx.get(f"{GATEWAY}/candidates/me", headers=auth).status_code == 200, what="the profile")
    httpx.put(f"{GATEWAY}/candidates/me", json={"full_name": "Jane Doe", "experience_years": 3}, headers=auth)
    httpx.post(f"{GATEWAY}/candidates/me/resumes", files={"file": ("cv.pdf", b"%PDF-1.4\n%%EOF\n")}, headers=auth)
    key = {"Idempotency-Key": str(uuid.uuid4())}
    r = httpx.post(f"{GATEWAY}/applications", json={"job_id": job["id"]}, headers={**auth, **key})
    assert r.status_code == 202, r.text
    again = httpx.post(f"{GATEWAY}/applications", json={"job_id": job["id"]}, headers={**auth, **key})
    assert again.json() == r.json()  # same key: the same answer, not a second application
    application = r.json()

    # The recruiter sees it on the board; ApplicationSubmitted reaches Kafka
    board = httpx.get(f"{GATEWAY}/jobs/{job['id']}/applications", headers=rec).json()["items"]
    assert [(a["id"], a["candidate_name"]) for a in board] == [(application["id"], "Jane Doe")]
    [msg] = read_messages("applications.lifecycle", application["id"])
    assert decode(msg)["payload"]["candidate_id"] == candidate_id

    # The recruiter closes the job: JobClosed reaches the replica, and nobody can apply anymore
    assert httpx.post(f"{GATEWAY}/jobs/{job['id']}/close", headers=rec).status_code == 200
    wait_for(lambda: sql(f"SELECT status FROM application.job_replica WHERE job_id = '{job['id']}'") == "CLOSED",
             what="the replica to close")
    _, other_token = register()
    other = {"Authorization": f"Bearer {other_token}", "Idempotency-Key": str(uuid.uuid4())}
    late = httpx.post(f"{GATEWAY}/applications", json={"job_id": job["id"]}, headers=other)
    assert late.status_code == 422 and late.json()["error"]["code"] == "JOB_NOT_OPEN"


# ---------- Phase 10: resume parsing (Celery) + scoring + the application workflow ----------


def candidate_with_parsed_resume(lines=None) -> tuple[str, dict]:
    """A candidate with a complete profile and a resume parsed by the Celery worker."""
    from resume_files import RESUME_LINES, make_pdf  # next to this file (pytest puts its folder on sys.path)

    candidate_id, token = register()
    auth = {"Authorization": f"Bearer {token}"}
    wait_for(lambda: httpx.get(f"{GATEWAY}/candidates/me", headers=auth).status_code == 200, what="the profile")
    # Eligibility (Application) uses the experience the candidate states in the profile
    httpx.put(f"{GATEWAY}/candidates/me", json={"full_name": "Jane Doe", "experience_years": 3}, headers=auth)
    pdf = make_pdf(lines or RESUME_LINES)
    assert httpx.post(f"{GATEWAY}/candidates/me/resumes", files={"file": ("cv.pdf", pdf, "application/pdf")},
                      headers=auth).status_code == 202
    wait_for(lambda: httpx.get(f"{GATEWAY}/candidates/me", headers=auth).json()["active_resume"]["status"] == "PARSED",
             timeout=30, what="the resume to be parsed")
    return candidate_id, auth


def test_uploaded_resume_is_parsed_in_the_background():
    candidate_id, _ = candidate_with_parsed_resume()
    events = [decode(m) for m in read_messages("candidates.profile", candidate_id, expected=2)]
    assert [e["event_type"] for e in events] == ["ResumeUploaded", "ResumeParsed"]
    parsed = events[1]["payload"]
    assert "python" in parsed["skills"] and parsed["experience_years"] >= 9  # 2017 - present, from the PDF


def test_application_is_scored_and_moved_to_screening():
    _, recruiter_token = register("RECRUITER")
    rec = {"Authorization": f"Bearer {recruiter_token}"}
    job = published_job(recruiter_token, required_skills=["Python", "Kubernetes", "Rust"],
                        eligibility_rules={"min_experience_years": 2})
    wait_for(lambda: sql(f"SELECT status FROM application.job_replica WHERE job_id = '{job['id']}'") == "READY",
             what="the replica")
    _, auth = candidate_with_parsed_resume()
    r = httpx.post(f"{GATEWAY}/applications", json={"job_id": job["id"]}, headers={**auth, "Idempotency-Key": str(uuid.uuid4())})
    assert r.status_code == 202, r.text
    application = r.json()

    def screened():
        board = httpx.get(f"{GATEWAY}/jobs/{job['id']}/applications", headers=rec).json()["items"]
        return board and board[0]["status"] == "SCREENING" and board[0]

    row = wait_for(screened, timeout=60, what="the workflow to score and screen the application")
    # must-haves: python + kubernetes ("k8s" in the resume) of 3 -> 70 * 2/3; other job skills (kafka, sql
    # from the description): none -> 0 of 10; experience 9 years >= 2 -> 20
    assert row["score_status"] == "SCORED" and row["score"] == round(70 * 2 / 3 + 0 + 20, 2)
    breakdown = httpx.get(f"http://localhost:8006/scores/{application['id']}").json()["breakdown"]
    assert breakdown["missing_skills"] == ["rust"]
    assert decode(read_messages("scoring.results", application["id"])[0])["event_type"] == "CandidateScored"
    history = httpx.get(f"{GATEWAY}/applications/{application['id']}", headers=auth).json()["history"]
    assert [h["to_stage"] for h in history] == ["SUBMITTED", "SCREENING"]


# ---------- Phase 11: notifications ----------

MAILPIT = "http://localhost:8025/api/v1"


def emails_to(user_id: str) -> list[str]:
    """Subjects of the emails Mailpit caught for this user (address looked up in Identity's DB)."""
    email = sql(f"SELECT email FROM identity.users WHERE id = '{user_id}'")
    found = httpx.get(f"{MAILPIT}/search", params={"query": f'to:"{email}"'}).json()["messages"]
    return sorted(m["Subject"] for m in found)


def test_hiring_flow_sends_emails_once():
    recruiter_id, recruiter_token = register("RECRUITER")
    job = published_job(recruiter_token, title="Data Engineer")
    wait_for(lambda: emails_to(recruiter_id) == ['Your job "Data Engineer" is live'], timeout=30, what="the JOB_READY email")
    wait_for(lambda: sql(f"SELECT status FROM application.job_replica WHERE job_id = '{job['id']}'") == "READY",
             what="the replica")

    candidate_id, auth = candidate_with_parsed_resume()
    r = httpx.post(f"{GATEWAY}/applications", json={"job_id": job["id"]}, headers={**auth, "Idempotency-Key": str(uuid.uuid4())})
    assert r.status_code == 202, r.text
    expected = ["We received your application for Data Engineer", "Your application for Data Engineer: SCREENING"]
    wait_for(lambda: emails_to(candidate_id) == expected, timeout=60, what="the candidate's two emails")

    # Replaying ApplicationSubmitted (the same bytes, as Kafka might) sends nothing new
    [msg] = read_messages("applications.lifecycle", r.json()["id"], expected=1)
    producer = Producer(KAFKA)
    producer.produce("applications.lifecycle", key=msg.key(), value=msg.value())
    producer.flush(10)
    time.sleep(5)
    assert emails_to(candidate_id) == expected


# ---------- Phase 12: analytics (CQRS read model) ----------


def metrics(token: str) -> dict:
    return httpx.get(f"{GATEWAY}/analytics/metrics", headers={"Authorization": f"Bearer {token}"}).json()


def fresh_metrics(token: str, check, what: str) -> dict:
    """Metrics are cached ~10 s and fed asynchronously: poll until the check passes."""
    return wait_for(lambda: (m := metrics(token)) and check(m) and m, timeout=40, what=what)


def test_metrics_follow_what_happens():
    _, recruiter_token = register("RECRUITER")
    time.sleep(12)  # let earlier tests' events land and the 10 s metrics cache expire: a clean "before"
    before = fresh_metrics(recruiter_token, lambda m: m["total_recruiters"] > 0, "the first metrics")
    job = published_job(recruiter_token, title="Analytics Engineer")
    _, auth = candidate_with_parsed_resume()
    wait_for(lambda: sql(f"SELECT status FROM application.job_replica WHERE job_id = '{job['id']}'") == "READY",
             what="the replica")
    r = httpx.post(f"{GATEWAY}/applications", json={"job_id": job["id"]}, headers={**auth, "Idempotency-Key": str(uuid.uuid4())})
    assert r.status_code == 202, r.text

    after = fresh_metrics(recruiter_token, lambda m: m["total_applications"] == before["total_applications"] + 1,
                          "the application to be counted")
    assert after["total_candidates"] == before["total_candidates"] + 1
    assert after["total_recruiters"] == before["total_recruiters"]  # registered before the snapshot
    assert after["total_jobs_published"] == before["total_jobs_published"] + 1
    assert any(j["job_id"] == job["id"] and j["applications"] == 1 for j in after["most_applied_jobs"]) or \
        len(after["most_applied_jobs"]) == 5  # in the top 5 unless 5 jobs already have more
    assert httpx.get(f"{GATEWAY}/analytics/metrics", headers=auth).status_code == 403  # candidates: no


def test_rebuild_from_events_gives_identical_numbers():
    """Step 12.4: empty the read model, replay every topic from the start, compare."""
    _, token = register("RECRUITER")
    time.sleep(12)  # let in-flight events land and the 10 s cache expire
    before = metrics(token)
    subprocess.run(["docker", "compose", "stop", "analytics"], check=True, capture_output=True)
    try:
        out = subprocess.run(["docker", "compose", "run", "--rm", "-T", "analytics", "python", "-m", "app.cli", "rebuild"],
                             check=True, capture_output=True, text=True).stdout
        assert "rebuilt the analytics read model from" in out
    finally:
        subprocess.run(["docker", "compose", "start", "analytics"], check=True, capture_output=True)
    wait_for(lambda: httpx.get("http://localhost:8008/health/ready").status_code == 200, what="analytics to restart")
    after = metrics(token)
    assert after == before


# ---------- Phase 12B: the BFF (composed screens) ----------


def test_bff_application_view_degrades_without_scoring():
    _, recruiter_token = register("RECRUITER")
    rec = {"Authorization": f"Bearer {recruiter_token}"}
    job = published_job(recruiter_token, title="Platform Engineer")
    wait_for(lambda: sql(f"SELECT status FROM application.job_replica WHERE job_id = '{job['id']}'") == "READY",
             what="the replica")
    candidate_id, auth = candidate_with_parsed_resume()
    application = httpx.post(f"{GATEWAY}/applications", json={"job_id": job["id"]},
                             headers={**auth, "Idempotency-Key": str(uuid.uuid4())}).json()
    view = wait_for(lambda: (v := httpx.get(f"{GATEWAY}/bff/applications/{application['id']}", headers=rec).json())
                    .get("score") and v, timeout=60, what="the composed view with a score")
    assert view["application"]["id"] == application["id"] and view["candidate"]["id"] == candidate_id
    assert view["degraded"] == [] and view["score"]["breakdown"]["matched_skills"]
    download = httpx.get(view["resume_download"]["url"])  # the presigned link works, no login, for 5 minutes
    assert download.status_code == 200 and download.content.startswith(b"%PDF-")
    # The candidate sees the same page for their own application (Application allows it)
    assert httpx.get(f"{GATEWAY}/bff/applications/{application['id']}", headers=auth).status_code == 200

    # Another recruiter: Application refuses (404: applications are private), the BFF passes it on
    _, other = register("RECRUITER")
    refused = httpx.get(f"{GATEWAY}/bff/applications/{application['id']}", headers={"Authorization": f"Bearer {other}"})
    assert refused.status_code == 404 and refused.json()["error"]["code"] == "APPLICATION_NOT_FOUND"

    # Scoring down: the page still loads, without the score
    subprocess.run(["docker", "compose", "stop", "scoring"], check=True, capture_output=True)
    try:
        degraded = httpx.get(f"{GATEWAY}/bff/applications/{application['id']}", headers=rec).json()
        assert degraded["score"] is None and degraded["degraded"] == ["score"] and degraded["candidate"] is not None
    finally:
        subprocess.run(["docker", "compose", "start", "scoring"], check=True, capture_output=True)

    # The recruiter dashboard: their job with its counts per stage, plus headline metrics
    dashboard = httpx.get(f"{GATEWAY}/bff/recruiter/dashboard", headers=rec).json()
    assert [(j["id"], j["total_applications"]) for j in dashboard["jobs"]] == [(job["id"], 1)]
    assert dashboard["metrics"]["total_applications"] >= 1 and dashboard["degraded"] == []


# ---------- Phase 13: one trace across HTTP, Kafka, Temporal and Celery ----------

JAEGER = "http://localhost:16686/api"


def jaeger_trace(trace_id: str) -> list[dict]:
    """Every span of the trace as {service, name, tags}; [] until Jaeger has it."""
    r = httpx.get(f"{JAEGER}/traces/{trace_id}")
    if r.status_code != 200 or not r.json()["data"]:
        return []
    trace = r.json()["data"][0]
    services = {key: p["serviceName"] for key, p in trace["processes"].items()}
    return [{"service": services[s["processID"]], "name": s["operationName"],
             "tags": {t["key"]: t["value"] for t in s["tags"]}} for s in trace["spans"]]


def test_one_trace_links_an_applications_workflow_and_its_events():
    """Step 13.8: for one application, its Temporal workflow AND the Kafka events it produced are in ONE
    trace. The test sends its own W3C traceparent, so it knows the trace id in advance."""
    _, recruiter_token = register("RECRUITER")
    job = published_job(recruiter_token, required_skills=["Python"])
    wait_for(lambda: sql(f"SELECT status FROM application.job_replica WHERE job_id = '{job['id']}'") == "READY",
             what="the replica")
    _, auth = candidate_with_parsed_resume()
    trace_id = uuid.uuid4().hex
    r = httpx.post(f"{GATEWAY}/applications", json={"job_id": job["id"]},
                   headers={**auth, "Idempotency-Key": str(uuid.uuid4()), "traceparent": f"00-{trace_id}-{uuid.uuid4().hex[:16]}-01"})
    assert r.status_code == 202, r.text
    application_id = r.json()["id"]

    def complete():  # the last hops (the SCREENING email, Analytics) and the gateway's span, which APISIX
        names = {(s["service"], s["name"]) for s in jaeger_trace(trace_id)}  # exports in batches, ~5-7 s late
        return {("notification-worker", "smtp send"), ("analytics", "process ApplicationStageChanged"),
                ("gateway", "POST /api/applications")} <= names
    wait_for(complete, timeout=90, what="the whole journey in Jaeger")
    spans = jaeger_trace(trace_id)
    names = {(s["service"], s["name"]) for s in spans}

    # HTTP: gateway -> application (-> candidate for the profile)
    assert {("gateway", "POST /api/applications"), ("application", "POST /applications"),
            ("candidate", "GET /candidates/me")} <= names
    # Kafka: the outbox relay published, three consumer groups processed, each span names its event id
    events = dict(row.split("|") for row in sql(
        f"SELECT (payload->>'event_type') || '|' || (payload->>'event_id') FROM application.outbox "
        f"WHERE message_key = '{application_id}'").splitlines())
    consumed = {s["tags"].get("messaging.message.id") for s in spans if s["name"].startswith("process ")}
    assert {events["ApplicationSubmitted"], events["ApplicationStageChanged"]} <= consumed
    assert {("application", "publish applications.lifecycle"), ("notification", "process ApplicationSubmitted"),
            ("analytics", "process ApplicationSubmitted"), ("application", "process ApplicationSubmitted")} <= names
    # Temporal: started by the consumer, ran its activities in two services, then the score event
    workflow = next(s for s in spans if s["name"] == "RunWorkflow:ApplicationWorkflow")
    assert workflow["tags"]["temporalWorkflowID"] == f"app-{application_id}"  # -> the Temporal UI / history
    assert {("application", "StartWorkflow:ApplicationWorkflow"), ("scoring", "RunActivity:score_application"),
            ("application", "RunActivity:record_score_and_advance"), ("scoring", "publish scoring.results")} <= names
    # Celery: the email task continued the same trace in the worker, down to SMTP
    assert {("notification", "apply_async/send_email"), ("notification-worker", "run/send_email")} <= names

    # ...and Temporal's own history of that workflow, found by the id the trace gave us
    described = json.loads(subprocess.run(
        ["docker", "compose", "exec", "-T", "temporal", "temporal", "workflow", "describe", "-w", f"app-{application_id}", "-o", "json"],
        check=True, capture_output=True, text=True).stdout)
    assert described["workflowExecutionInfo"]["status"] == "WORKFLOW_EXECUTION_STATUS_COMPLETED"
