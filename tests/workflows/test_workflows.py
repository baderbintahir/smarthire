"""Workflows end to end against the RUNNING stack (design v2.1): `make up`, then `make test-workflows`.

Real gateway and services, the real orchestrator and Temporal. These prove the wiring (actions, workflow
ids, internal endpoints, service tokens, facts, emails); the logic itself is unit-tested in each service
and in services/orchestrator (against a local Temporal dev server)."""

import hashlib
import json
import subprocess
import time
import uuid

import httpx
import pytest

GATEWAY = "http://localhost:8000/api"
MAILPIT = "http://localhost:8025/api/v1"
JAEGER = "http://localhost:16686/api"


@pytest.fixture(scope="session", autouse=True)
def stack_running():
    try:
        httpx.get(f"{GATEWAY}/identity/.well-known/jwks.json", timeout=3).raise_for_status()
    except httpx.HTTPError:
        pytest.skip("Stack not running: `make up` first")
    if compose("exec", "-T", "orchestrator", "python", "-c", "print(1)").returncode != 0:
        pytest.skip("The orchestrator is not running (run `make keys` once, then `make up`)")


# ---------- helpers ----------


def compose(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], capture_output=True, text=True)


def register(role="CANDIDATE", roles=None, full_name=None) -> tuple[str, str]:
    """Registers through the gateway; returns (user_id, access_token)."""
    user_id, login = register_and_login(role, roles, full_name)
    return user_id, login.json()["access_token"]


def register_and_login(role="CANDIDATE", roles=None, full_name=None) -> tuple[str, httpx.Response]:
    email = f"wf-{uuid.uuid4().hex[:10]}@example.com"
    body = {"email": email, "password": "correct-horse", "roles": roles or [role],
            "full_name": full_name or ("Rita Recruiter" if role == "RECRUITER" else "Jane Doe")}
    r = httpx.post(f"{GATEWAY}/identity/auth/register", json=body, timeout=15)
    assert r.status_code == 201, r.text
    login = httpx.post(f"{GATEWAY}/identity/auth/login", json={"email": email, "password": "correct-horse"})
    assert login.status_code == 200, login.text
    return r.json()["id"], login


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


def workflow(workflow_id: str) -> dict:
    """Temporal's description of a workflow (status, type), or {} if it doesn't exist (yet)."""
    out = compose("exec", "-T", "temporal", "temporal", "workflow", "describe", "-w", workflow_id, "-o", "json")
    return json.loads(out.stdout)["workflowExecutionInfo"] if out.returncode == 0 else {}


def completed(workflow_id: str, timeout: float = 60) -> dict:
    return wait_for(lambda: (info := workflow(workflow_id)).get("status") == "WORKFLOW_EXECUTION_STATUS_COMPLETED"
                    and info, timeout=timeout, what=f"workflow {workflow_id} to complete")


def facts_of(column: str, value: str) -> list[str]:
    """Kinds of the analytics facts whose data has this id (the facts table is Analytics' own history)."""
    rows = sql(f"SELECT kind FROM analytics.facts WHERE data->>'{column}' = '{value}' ORDER BY occurred_at")
    return rows.splitlines()


def published_job(token: str, **overrides) -> dict:
    """Create and publish a job through the gateway, then wait for the REAL JobPublishingWorkflow (in the
    orchestrator, steps in Job, Job Enrichment and Application) to make it READY."""
    auth = {"Authorization": f"Bearer {token}"}
    body = {"title": "Data Engineer", "description": "You will build Kafka pipelines in Python.\nRequirements:\n- 2 years of SQL",
            "employment_type": "FULL_TIME", "stages": ["Screening", "Interview"], "required_skills": ["Python"], **overrides}
    job = httpx.post(f"{GATEWAY}/jobs", json=body, headers=auth).json()
    assert httpx.post(f"{GATEWAY}/jobs/{job['id']}/publish", headers=auth, timeout=15).status_code == 202
    ready = wait_for(lambda: (j := httpx.get(f"{GATEWAY}/jobs/{job['id']}", headers=auth).json())["status"] == "READY" and j,
                     timeout=60, what="the job to become READY")
    wait_for(lambda: sql(f"SELECT status FROM application.job_replica WHERE job_id = '{job['id']}'") == "READY",
             what="Application's replica")
    return ready


def candidate_with_parsed_resume(lines=None) -> tuple[str, dict]:
    """A candidate with a complete profile and a resume parsed by candidate-parser."""
    from resume_files import RESUME_LINES, make_pdf  # next to this file (pytest puts its folder on sys.path)

    candidate_id, token = register()
    auth = {"Authorization": f"Bearer {token}"}
    wait_for(lambda: httpx.get(f"{GATEWAY}/candidates/me", headers=auth).status_code == 200, what="the profile")
    httpx.put(f"{GATEWAY}/candidates/me", json={"experience_years": 3}, headers=auth)  # eligibility reads this
    pdf = make_pdf(lines or RESUME_LINES)
    assert httpx.post(f"{GATEWAY}/candidates/me/resumes", files={"file": ("cv.pdf", pdf, "application/pdf")},
                      headers=auth, timeout=15).status_code == 202
    wait_for(lambda: httpx.get(f"{GATEWAY}/candidates/me", headers=auth).json()["active_resume"]["status"] == "PARSED",
             timeout=30, what="the resume to be parsed")
    return candidate_id, auth


def apply(auth: dict, job_id: str, key: str | None = None, **headers) -> httpx.Response:
    return httpx.post(f"{GATEWAY}/applications", json={"job_id": job_id}, timeout=15,
                      headers={**auth, "Idempotency-Key": key or str(uuid.uuid4()), **headers})


def emails_to(user_id: str) -> list[str]:
    """Subjects of the emails Mailpit caught for this user (address looked up in Identity's DB)."""
    email = sql(f"SELECT email FROM identity.users WHERE id = '{user_id}'")
    found = httpx.get(f"{MAILPIT}/search", params={"query": f'to:"{email}"'}).json()["messages"]
    return sorted(m["Subject"] for m in found)


# ---------- W1, W2: registration and roles ----------


def test_registering_a_candidate_runs_registration_workflow():
    user_id, login = register_and_login()
    email = sql(f"SELECT email FROM identity.users WHERE id = '{user_id}'")
    info = completed(f"register-{hashlib.sha256(email.encode()).hexdigest()}")
    assert info["type"]["name"] == "RegistrationWorkflow"
    auth = {"Authorization": f"Bearer {login.json()['access_token']}"}
    assert httpx.get(f"{GATEWAY}/candidates/me", headers=auth).status_code == 200  # the profile step ran
    assert wait_for(lambda: facts_of("user_id", user_id), what="the UserRegistered fact") == ["UserRegistered"]


def test_recruiters_get_no_candidate_profile():
    user_id, _ = register("RECRUITER")
    wait_for(lambda: facts_of("user_id", user_id), what="the fact")  # the workflow finished...
    assert sql(f"SELECT count(*) FROM candidate.candidates WHERE id = '{user_id}'") == "0"  # ...without a profile


def test_orchestrator_down_means_503_and_nothing_written():
    """Design v2.0's promise: an action either goes all the way or doesn't happen at all."""
    email = f"wf-down-{uuid.uuid4().hex[:8]}@example.com"
    assert compose("stop", "orchestrator").returncode == 0
    try:
        r = httpx.post(f"{GATEWAY}/identity/auth/register", timeout=20, json={
            "email": email, "password": "correct-horse", "roles": ["CANDIDATE"], "full_name": "Down Test"})
        assert r.status_code == 503 and r.json()["error"]["code"] == "WORKFLOWS_UNAVAILABLE", r.text
        assert r.headers["Retry-After"] == "5"
    finally:
        compose("start", "orchestrator")
    assert sql(f"SELECT count(*) FROM identity.users WHERE email = '{email}'") == "0"
    wait_for(lambda: compose("exec", "-T", "orchestrator", "python", "-c",
                             "import urllib.request; urllib.request.urlopen('http://localhost:8012/health/ready')"
                             ).returncode == 0, timeout=60, what="the orchestrator to be ready again")


def test_recruiter_takes_candidate_role_but_cannot_apply_to_own_job():
    """Phase 14R "Done when", now through RoleChangeWorkflow: add CANDIDATE -> refresh -> own-job apply 403."""
    rita_id, login = register_and_login("RECRUITER", full_name="Rita Both")
    rita = {"Authorization": f"Bearer {login.json()['access_token']}"}
    r = httpx.post(f"{GATEWAY}/identity/users/me/roles", json={"role": "CANDIDATE"}, headers=rita, timeout=15)
    assert r.status_code == 201 and r.json()["roles"] == ["CANDIDATE", "RECRUITER"]
    completed(f"role-{rita_id}-CANDIDATE-grant")
    assert facts_of("user_id", rita_id) == ["UserRegistered", "RoleGranted"]

    # Refresh through the gateway: the cookie is Secure, and the test talks plain http to localhost
    refreshed = httpx.post(f"{GATEWAY}/identity/auth/refresh",
                           headers={"Cookie": f"refresh_token={login.cookies['refresh_token']}"})
    assert refreshed.status_code == 200, refreshed.text
    rita = {"Authorization": f"Bearer {refreshed.json()['access_token']}"}
    assert httpx.get(f"{GATEWAY}/candidates/me", headers=rita).status_code == 200  # the grant made her profile
    httpx.put(f"{GATEWAY}/candidates/me", json={"experience_years": 3}, headers=rita)
    httpx.post(f"{GATEWAY}/candidates/me/resumes", files={"file": ("cv.pdf", b"%PDF-1.4\n%%EOF\n")}, headers=rita)

    job = published_job(refreshed.json()["access_token"])
    own = apply(rita, job["id"])
    assert own.status_code == 403 and own.json()["error"]["code"] == "CANNOT_APPLY_TO_OWN_JOB"


# ---------- W6, W7: publishing and closing ----------


def test_publishing_runs_the_saga_to_ready():
    recruiter_id, token = register("RECRUITER")
    job = published_job(token)
    info = completed(f"publish-{job['id']}-v1")
    assert info["type"]["name"] == "JobPublishingWorkflow"
    structured = httpx.get(f"http://localhost:8003/jobs/{job['id']}/structured").json()
    assert structured["skills"][:1] == ["python"] and "kafka" in structured["skills"]
    assert structured["sections"]["requirements"] == ["2 years of SQL"]
    assert wait_for(lambda: facts_of("job_id", job["id"]), what="the JobReady fact") == ["JobReady"]
    wait_for(lambda: emails_to(recruiter_id) == ['Your job "Data Engineer" is live'], timeout=30,
             what="the JOB_READY email")


def test_apply_close_and_nobody_else_can_apply():
    recruiter_id, recruiter_token = register("RECRUITER")
    rec = {"Authorization": f"Bearer {recruiter_token}"}
    job = published_job(recruiter_token, eligibility_rules={"min_experience_years": 2})

    candidate_id, token = register()
    auth = {"Authorization": f"Bearer {token}"}
    wait_for(lambda: httpx.get(f"{GATEWAY}/candidates/me", headers=auth).status_code == 200, what="the profile")
    httpx.put(f"{GATEWAY}/candidates/me", json={"experience_years": 3}, headers=auth)
    httpx.post(f"{GATEWAY}/candidates/me/resumes", files={"file": ("cv.pdf", b"%PDF-1.4\n%%EOF\n")}, headers=auth)
    key = str(uuid.uuid4())
    r = apply(auth, job["id"], key)
    assert r.status_code == 202, r.text
    assert apply(auth, job["id"], key).json() == r.json()  # same key: the same answer, not a second application
    second = apply(auth, job["id"])  # another key: another apply for the same job
    assert second.status_code == 409 and second.json()["error"]["code"] == "ALREADY_APPLIED"
    application = r.json()
    board = httpx.get(f"{GATEWAY}/jobs/{job['id']}/applications", headers=rec).json()["items"]
    assert [(a["id"], a["candidate_name"]) for a in board] == [(application["id"], "Jane Doe")]

    assert httpx.post(f"{GATEWAY}/jobs/{job['id']}/close", headers=rec, timeout=15).status_code == 200
    completed(f"close-{job['id']}")
    assert sql(f"SELECT status FROM application.job_replica WHERE job_id = '{job['id']}'") == "CLOSED"
    _, other_token = register()
    late = apply({"Authorization": f"Bearer {other_token}"}, job["id"])
    assert late.status_code == 422 and late.json()["error"]["code"] == "JOB_NOT_OPEN"


# ---------- W9-W11, W14: resumes, scoring, emails ----------


def test_uploaded_resume_is_parsed_by_candidate_parser():
    candidate_id, auth = candidate_with_parsed_resume()
    resume = httpx.get(f"{GATEWAY}/candidates/me/resumes", headers=auth).json()["items"][0]
    assert completed(f"resume-{resume['id']}")["type"]["name"] == "ResumeProcessingWorkflow"
    parsed = json.loads(sql(f"SELECT parsed_data FROM candidate.resumes WHERE id = '{resume['id']}'"))
    assert "python" in parsed["skills"] and parsed["experience_years"] >= 9  # 2017 - present, from the PDF


def test_application_is_scored_screened_and_emailed_once():
    recruiter_id, recruiter_token = register("RECRUITER")
    rec = {"Authorization": f"Bearer {recruiter_token}"}
    job = published_job(recruiter_token, required_skills=["Python", "Kubernetes", "Rust"],
                        eligibility_rules={"min_experience_years": 2})
    candidate_id, auth = candidate_with_parsed_resume()
    key = str(uuid.uuid4())
    r = apply(auth, job["id"], key)
    assert r.status_code == 202, r.text
    application = r.json()

    info = completed(f"application-{job['id']}-{candidate_id}")
    assert info["type"]["name"] == "ApplicationWorkflow"
    row = httpx.get(f"{GATEWAY}/jobs/{job['id']}/applications", headers=rec).json()["items"][0]
    # must-haves: python + kubernetes ("k8s" in the resume) of 3 -> 70 * 2/3; other job skills (kafka, sql
    # from the description): none -> 0 of 10; experience 9 years >= 2 -> 20
    assert (row["status"], row["score_status"], row["score"]) == ("SCREENING", "SCORED", round(70 * 2 / 3 + 0 + 20, 2))
    assert httpx.get(f"http://localhost:8006/scores/{application['id']}").json()["breakdown"]["missing_skills"] == ["rust"]
    history = httpx.get(f"{GATEWAY}/applications/{application['id']}", headers=auth).json()["history"]
    assert [h["to_stage"] for h in history] == ["SUBMITTED", "SCREENING"]
    assert facts_of("application_id", application["id"]) == ["ApplicationSubmitted", "CandidateScored",
                                                             "ApplicationStageChanged"]

    expected = ["We received your application for Data Engineer", "Your application for Data Engineer: SCREENING"]
    wait_for(lambda: emails_to(candidate_id) == expected, timeout=60, what="the candidate's two emails")
    apply(auth, job["id"], key)  # the same request again: answered from the Idempotency-Key, nothing re-runs
    time.sleep(5)
    assert emails_to(candidate_id) == expected


def test_a_step_whose_service_is_down_waits_then_finishes():
    """Scoring stopped: the application is accepted (202) and stays SUBMITTED; once Scoring is back, the
    orchestrator's retries finish the workflow. Nothing is lost while a service is down."""
    _, recruiter_token = register("RECRUITER")
    rec = {"Authorization": f"Bearer {recruiter_token}"}
    job = published_job(recruiter_token)
    candidate_id, auth = candidate_with_parsed_resume()
    assert compose("stop", "scoring").returncode == 0
    try:
        assert apply(auth, job["id"]).status_code == 202
        time.sleep(8)
        board = httpx.get(f"{GATEWAY}/jobs/{job['id']}/applications", headers=rec).json()["items"]
        assert board[0]["status"] == "SUBMITTED"  # waiting for the score
    finally:
        compose("start", "scoring")
    completed(f"application-{job['id']}-{candidate_id}", timeout=120)  # backoff grows to a minute between tries
    assert httpx.get(f"{GATEWAY}/jobs/{job['id']}/applications", headers=rec).json()["items"][0]["status"] == "SCREENING"


# ---------- W15: analytics ----------


def metrics(token: str) -> dict:
    return httpx.get(f"{GATEWAY}/analytics/metrics", headers={"Authorization": f"Bearer {token}"}).json()


def test_metrics_follow_what_happens():
    _, recruiter_token = register("RECRUITER")
    time.sleep(12)  # let earlier tests' facts land and the 10 s metrics cache expire: a clean "before"
    before = metrics(recruiter_token)
    job = published_job(recruiter_token, title="Analytics Engineer")
    _, auth = candidate_with_parsed_resume()
    assert apply(auth, job["id"]).status_code == 202
    after = wait_for(lambda: (m := metrics(recruiter_token))["total_applications"] == before["total_applications"] + 1
                     and m, timeout=40, what="the application to be counted")
    assert after["total_candidates"] == before["total_candidates"] + 1
    assert after["total_jobs_published"] == before["total_jobs_published"] + 1
    assert httpx.get(f"{GATEWAY}/analytics/metrics", headers=auth).status_code == 403  # candidates: no


def test_rebuild_from_facts_gives_identical_numbers():
    """The read model is a view of Analytics' own facts table: rebuild it (no need to stop the service)."""
    _, token = register("RECRUITER")
    time.sleep(12)  # let in-flight facts land and the 10 s cache expire
    before = metrics(token)
    out = compose("exec", "-T", "analytics", "python", "-m", "app.cli", "rebuild")
    assert out.returncode == 0 and "rebuilt the analytics read model from" in out.stdout, out.stderr
    assert metrics(token) == before


# ---------- W16: the BFF ----------


def test_bff_application_view_degrades_without_scoring():
    _, recruiter_token = register("RECRUITER")
    rec = {"Authorization": f"Bearer {recruiter_token}"}
    job = published_job(recruiter_token, title="Platform Engineer")
    candidate_id, auth = candidate_with_parsed_resume()
    application = apply(auth, job["id"]).json()
    view = wait_for(lambda: (v := httpx.get(f"{GATEWAY}/bff/applications/{application['id']}", headers=rec).json())
                    .get("score") and v, timeout=60, what="the composed view with a score")
    assert view["application"]["id"] == application["id"] and view["candidate"]["id"] == candidate_id
    assert view["degraded"] == [] and view["score"]["breakdown"]["matched_skills"]
    download = httpx.get(view["resume_download"]["url"])  # the presigned link works, no login, for 5 minutes
    assert download.status_code == 200 and download.content.startswith(b"%PDF-")

    compose("stop", "scoring")
    try:
        degraded = httpx.get(f"{GATEWAY}/bff/applications/{application['id']}", headers=rec).json()
        assert degraded["score"] is None and degraded["degraded"] == ["score"] and degraded["candidate"] is not None
    finally:
        compose("start", "scoring")


# ---------- one trace across HTTP, the orchestrator, Temporal and the services' internal endpoints ----------


def jaeger_trace(trace_id: str) -> set[tuple[str, str]]:
    """(service, span name) of every span of the trace; empty until Jaeger has it."""
    r = httpx.get(f"{JAEGER}/traces/{trace_id}")
    if r.status_code != 200 or not r.json()["data"]:
        return set()
    trace = r.json()["data"][0]
    services = {key: p["serviceName"] for key, p in trace["processes"].items()}
    return {(services[s["processID"]], s["operationName"]) for s in trace["spans"]}


def test_one_trace_follows_an_apply_through_the_orchestrator():
    """The test sends its own W3C traceparent, so it knows the trace id: the apply request, the
    orchestrator's workflow and every step it called in other services are one trace."""
    _, recruiter_token = register("RECRUITER")
    job = published_job(recruiter_token, required_skills=["Python"])
    _, auth = candidate_with_parsed_resume()
    trace_id = uuid.uuid4().hex
    r = apply(auth, job["id"], traceparent=f"00-{trace_id}-{uuid.uuid4().hex[:16]}-01")
    assert r.status_code == 202, r.text

    wanted = {
        ("gateway", "POST /api/applications"), ("application", "POST /applications"),  # the request
        ("orchestrator", "POST /actions/{action}"),  # Application asks the orchestrator
        ("application", "POST /internal/applications"),  # the commit step, back in Application
        ("scoring", "POST /internal/scores"),  # a follow-up step
        ("analytics", "POST /internal/facts"),  # the facts
    }
    found = wait_for(lambda: wanted <= (spans := jaeger_trace(trace_id)) and spans, timeout=90,
                     what="the whole journey in Jaeger")
    assert any(service == "orchestrator" and name.startswith("RunWorkflow:ApplicationWorkflow") for service, name in found)
