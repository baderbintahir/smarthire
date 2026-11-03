"""Events end to end against the RUNNING stack: `make up`, then `make test-events`.

Real Identity/Job/Candidate, real outbox relays, real Kafka + Schema Registry, real consumer. These
prove the wiring (topics, keys, schemas, consumer group); the logic itself is unit-tested in
libs/smarthire_common/tests and in each service."""

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
        if result := check():
            return result
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
    key = f"poison-{uuid.uuid4()}"
    producer = Producer(KAFKA)
    producer.produce("identity.users", key=key, value=b"this is not an event")
    producer.flush(10)
    [dead] = read_messages("identity.users.dlq", key)
    headers = dict(dead.headers())
    assert dead.value() == b"this is not an event"  # kept unchanged for a human to inspect
    assert headers["x-consumer"] == b"candidate" and headers["x-original-topic"] == b"identity.users"
    assert headers["x-error"]


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
