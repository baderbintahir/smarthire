"""Steps 8.3-8.5 against the real app (Postgres + Redis), with a fake Candidate API and tokens from a
test key. Jobs are put straight into job_replica (in production, Job's events put them there)."""

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from smarthire_common import testing
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import MAX_ACTIVE

STAGES = ["Screening", "Interview", "Offer"]


def db(database_url, sql, **params):
    async def go():
        engine = create_async_engine(database_url)
        async with engine.begin() as conn:
            result = await conn.execute(text(sql), params)
            rows = result.mappings().all() if result.returns_rows else None
        await engine.dispose()
        return rows

    return testing.run(go())


@pytest.fixture
def add_job(database_url):
    def add(recruiter_id=None, status="READY", rules="{}", deadline=None) -> str:
        job_id = str(uuid.uuid4())
        db(database_url, "INSERT INTO job_replica (job_id, recruiter_id, title, status, stages, eligibility_rules, "
                         "deadline, source_version) VALUES (:id, :rid, 'Backend Engineer', :status, CAST(:stages AS jsonb), "
                         "CAST(:rules AS jsonb), :deadline, 1)",
           id=job_id, rid=str(recruiter_id or uuid.uuid4()), status=status, stages='["Screening", "Interview", "Offer"]',
           rules=rules, deadline=deadline)
        return job_id

    return add


@pytest.fixture
def candidate(tokens):
    user_id = uuid.uuid4()
    return user_id, tokens.headers(user_id=user_id, role="CANDIDATE")


@pytest.fixture
def recruiter(tokens):
    user_id = uuid.uuid4()
    return user_id, tokens.headers(user_id=user_id, role="RECRUITER")


def apply(client, headers, job_id, key=None):
    return client.post("/applications", json={"job_id": job_id},
                       headers={**headers, "Idempotency-Key": key or str(uuid.uuid4())})


# ---------------------------------------------------------------- 8.3 submit


def test_apply_creates_application_history_quota_and_event(client, candidate, recruiter, add_job, database_url):
    user_id, headers = candidate
    job_id = add_job(recruiter[0])
    r = apply(client, headers, job_id)
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["status"] == "SUBMITTED" and body["current_stage"] == "SUBMITTED"
    assert body["candidate_name"] == "Jane Doe" and body["job_title"] == "Backend Engineer"

    [history] = db(database_url, "SELECT from_stage, to_stage FROM application_stage_history")
    assert (history["from_stage"], history["to_stage"]) == (None, "SUBMITTED")
    assert db(database_url, "SELECT active_count FROM application_quotas")[0]["active_count"] == 1
    [event] = db(database_url, "SELECT message_key, payload FROM outbox")
    assert event["message_key"] == body["id"] and event["payload"]["event_type"] == "ApplicationSubmitted"
    assert event["payload"]["payload"] == {"application_id": body["id"], "job_id": job_id, "candidate_id": str(user_id),
                                           "recruiter_id": str(recruiter[0]), "job_title": "Backend Engineer"}


def test_same_idempotency_key_returns_the_same_response(client, candidate, add_job, candidates, database_url):
    job_id = add_job()
    first = apply(client, candidate[1], job_id, key="attempt-1")
    again = apply(client, candidate[1], job_id, key="attempt-1")  # a retry after a timeout
    assert (first.status_code, again.status_code) == (202, 202)
    assert first.json() == again.json()
    assert candidates.calls == 1  # the retry never ran the use case again
    assert db(database_url, "SELECT count(*) AS n FROM applications")[0]["n"] == 1


def test_different_keys_for_the_same_job_is_409(client, candidate, add_job, database_url):
    job_id = add_job()
    assert apply(client, candidate[1], job_id).status_code == 202
    r = apply(client, candidate[1], job_id)
    assert r.status_code == 409 and r.json()["error"]["code"] == "ALREADY_APPLIED"
    assert db(database_url, "SELECT active_count FROM application_quotas")[0]["active_count"] == 1  # rolled back


def test_reusing_a_key_for_another_job_is_422(client, candidate, add_job):
    apply(client, candidate[1], add_job(), key="k")
    r = apply(client, candidate[1], add_job(), key="k")
    assert r.status_code == 422 and r.json()["error"]["code"] == "IDEMPOTENCY_KEY_REUSED"


def test_idempotency_keys_are_per_user(client, candidate, tokens, add_job):
    job_id = add_job()
    assert apply(client, candidate[1], job_id, key="shared").status_code == 202
    other = tokens.headers(role="CANDIDATE")
    r = apply(client, other, job_id, key="shared")  # same key, different user: their own application
    assert r.status_code == 202 and r.json()["candidate_id"] != str(candidate[0])


def test_idempotency_key_is_required(client, candidate, add_job):
    r = client.post("/applications", json={"job_id": add_job()}, headers=candidate[1])
    assert r.status_code == 422


def test_only_candidates_apply(client, recruiter, add_job):
    assert apply(client, recruiter[1], add_job()).status_code == 403
    assert client.post("/applications", json={"job_id": add_job()}, headers={"Idempotency-Key": "k"}).status_code == 401


@pytest.mark.parametrize("job", [{"status": "DRAFT"}, {"status": "CLOSED"}, {"deadline": "past"}, None])
def test_job_must_be_open(client, candidate, add_job, job):
    if job is None:
        job_id = str(uuid.uuid4())  # not in our replica at all
    elif job.get("deadline") == "past":
        job_id = add_job(deadline=datetime.now(UTC) - timedelta(minutes=1))
    else:
        job_id = add_job(**job)
    r = apply(client, candidate[1], job_id)
    assert r.status_code == 422 and r.json()["error"]["code"] == "JOB_NOT_OPEN"


def test_not_eligible_lists_reasons_and_frees_the_key(client, candidate, candidates, add_job):
    user_id, headers = candidate
    job_id = add_job(rules='{"min_experience_years": 7}')
    candidates.set(user_id, experience_years=None)
    r = apply(client, headers, job_id, key="try")
    assert r.status_code == 422 and r.json()["error"]["details"] == {"reasons": ["at least 7 years of experience required"]}
    candidates.set(user_id, experience_years=8)  # the candidate updates their profile...
    assert apply(client, headers, job_id, key="try").status_code == 202  # ...and retries with the same key


def test_incomplete_profile_is_422(client, candidate, candidates, add_job):
    candidates.set(candidate[0], has_resume=False)
    r = apply(client, candidate[1], add_job())
    assert r.status_code == 422 and r.json()["error"]["code"] == "PROFILE_INCOMPLETE"


def test_candidate_service_down_is_503(client, candidate, candidates, add_job):
    candidates.down = True
    assert apply(client, candidate[1], add_job()).status_code == 503


def test_quota_limit_is_422(client, candidate, add_job, database_url):
    for _ in range(MAX_ACTIVE):
        assert apply(client, candidate[1], add_job()).status_code == 202
    r = apply(client, candidate[1], add_job())
    assert r.status_code == 422 and r.json()["error"]["code"] == "QUOTA_EXCEEDED"


def test_concurrent_applies_never_exceed_the_quota(client, candidate, add_job, database_url):
    """Break it (Step 8.3): 20 simultaneous applies to 20 different jobs. The quota row lock makes them
    take turns, so exactly MAX_ACTIVE succeed."""
    job_ids = [add_job() for _ in range(20)]
    with ThreadPoolExecutor(20) as pool:
        statuses = list(pool.map(lambda j: apply(client, candidate[1], j).status_code, job_ids))
    assert statuses.count(202) == MAX_ACTIVE and statuses.count(422) == 20 - MAX_ACTIVE
    assert db(database_url, "SELECT active_count FROM application_quotas")[0]["active_count"] == MAX_ACTIVE
    assert db(database_url, "SELECT count(*) AS n FROM applications")[0]["n"] == MAX_ACTIVE


def test_concurrent_applies_to_one_job_create_one_application(client, candidate, add_job, database_url):
    job_id = add_job()
    with ThreadPoolExecutor(10) as pool:
        statuses = list(pool.map(lambda _: apply(client, candidate[1], job_id).status_code, range(10)))
    assert statuses.count(202) == 1 and statuses.count(409) == 9
    assert db(database_url, "SELECT active_count FROM application_quotas")[0]["active_count"] == 1


# ---------------------------------------------------------------- 8.4 views


def test_my_applications_newest_first_with_pages(client, candidate, tokens, add_job):
    ids = [apply(client, candidate[1], add_job()).json()["id"] for _ in range(3)]
    apply(client, tokens.headers(role="CANDIDATE"), add_job())  # someone else's
    page1 = client.get("/applications/me?limit=2", headers=candidate[1]).json()
    page2 = client.get(f"/applications/me?limit=2&cursor={page1['next_cursor']}", headers=candidate[1]).json()
    assert [a["id"] for a in page1["items"] + page2["items"]] == ids[::-1]
    assert page2["next_cursor"] is None


def test_board_is_for_the_job_owner_sorted_by_score(client, recruiter, tokens, add_job, database_url):
    job_id = add_job(recruiter[0])
    apps = [apply(client, tokens.headers(role="CANDIDATE"), job_id).json()["id"] for _ in range(3)]
    db(database_url, "UPDATE applications SET score = 90 WHERE id = :id", id=apps[1])
    db(database_url, "UPDATE applications SET score = 40 WHERE id = :id", id=apps[2])
    board = client.get(f"/jobs/{job_id}/applications", headers=recruiter[1]).json()["items"]
    assert [a["id"] for a in board] == [apps[1], apps[2], apps[0]]  # best first, unscored last
    other = tokens.headers(role="RECRUITER")
    assert client.get(f"/jobs/{job_id}/applications", headers=other).status_code == 404
    assert client.get(f"/jobs/{uuid.uuid4()}/applications", headers=recruiter[1]).status_code == 404


def test_board_filters_by_stage(client, recruiter, candidate, add_job):
    job_id = add_job(recruiter[0])
    app = apply(client, candidate[1], job_id).json()
    client.patch(f"/applications/{app['id']}/stage", json={"to_stage": "Interview", "version": 1}, headers=recruiter[1])
    assert len(client.get(f"/jobs/{job_id}/applications?stage=Interview", headers=recruiter[1]).json()["items"]) == 1
    assert client.get(f"/jobs/{job_id}/applications?stage=Offer", headers=recruiter[1]).json()["items"] == []


def test_detail_visibility(client, candidate, recruiter, tokens, add_job):
    app = apply(client, candidate[1], add_job(recruiter[0])).json()
    client.post(f"/applications/{app['id']}/notes", json={"body": "Strong SQL"}, headers=recruiter[1])
    as_recruiter = client.get(f"/applications/{app['id']}", headers=recruiter[1]).json()
    as_candidate = client.get(f"/applications/{app['id']}", headers=candidate[1]).json()
    assert [n["body"] for n in as_recruiter["notes"]] == ["Strong SQL"]
    assert as_candidate["notes"] is None  # recruiter notes are never shown to the candidate
    assert [h["to_stage"] for h in as_candidate["history"]] == ["SUBMITTED"]
    for stranger in (tokens.headers(role="CANDIDATE"), tokens.headers(role="RECRUITER")):
        assert client.get(f"/applications/{app['id']}", headers=stranger).status_code == 404


# ---------------------------------------------------------------- 8.5 changes


def test_stage_change_writes_history_audit_and_event(client, candidate, recruiter, add_job, database_url):
    app = apply(client, candidate[1], add_job(recruiter[0])).json()
    r = client.patch(f"/applications/{app['id']}/stage", json={"to_stage": "Interview", "version": 1, "reason": "good CV"},
                     headers=recruiter[1])
    assert r.status_code == 200, r.text
    assert (r.json()["status"], r.json()["current_stage"], r.json()["version"]) == ("IN_PROGRESS", "Interview", 2)
    [audit] = db(database_url, "SELECT action, before, after FROM audit_entries")
    assert (audit["action"], audit["before"], audit["after"]) == ("STAGE_CHANGED", {"stage": "SUBMITTED"}, {"stage": "Interview"})
    event = db(database_url, "SELECT payload FROM outbox ORDER BY id DESC LIMIT 1")[0]["payload"]
    assert event["event_type"] == "ApplicationStageChanged"
    assert (event["payload"]["from_stage"], event["payload"]["to_stage"]) == ("SUBMITTED", "Interview")


def test_stage_change_rules(client, candidate, recruiter, tokens, add_job):
    app = apply(client, candidate[1], add_job(recruiter[0])).json()
    url = f"/applications/{app['id']}/stage"
    assert client.patch(url, json={"to_stage": "Lunch", "version": 1}, headers=recruiter[1]).status_code == 422
    assert client.patch(url, json={"to_stage": "Interview", "version": 1}, headers=candidate[1]).status_code == 403
    assert client.patch(url, json={"to_stage": "Interview", "version": 1},
                        headers=tokens.headers(role="RECRUITER")).status_code == 404  # not their job
    assert client.patch(url, json={"to_stage": "Interview", "version": 1}, headers=recruiter[1]).status_code == 200
    stale = client.patch(url, json={"to_stage": "Offer", "version": 1}, headers=recruiter[1])  # read v1, it's v2 now
    assert stale.status_code == 409 and stale.json()["error"]["code"] == "VERSION_CONFLICT"


def test_decision_is_final_frees_quota_and_publishes(client, candidate, recruiter, add_job, database_url):
    app = apply(client, candidate[1], add_job(recruiter[0])).json()
    r = client.post(f"/applications/{app['id']}/decision", json={"decision": "HIRED", "version": 1}, headers=recruiter[1])
    assert r.status_code == 200 and r.json()["status"] == "HIRED"
    assert db(database_url, "SELECT active_count FROM application_quotas")[0]["active_count"] == 0
    event = db(database_url, "SELECT payload FROM outbox ORDER BY id DESC LIMIT 1")[0]["payload"]
    assert event["event_type"] == "ApplicationDecisionMade" and event["payload"]["decision"] == "HIRED"
    again = client.post(f"/applications/{app['id']}/decision", json={"decision": "REJECTED", "version": 2},
                        headers=recruiter[1])
    assert again.status_code == 409 and again.json()["error"]["code"] == "APPLICATION_NOT_ACTIVE"


def test_notes_are_audited(client, candidate, recruiter, add_job, database_url):
    app = apply(client, candidate[1], add_job(recruiter[0])).json()
    r = client.post(f"/applications/{app['id']}/notes", json={"body": "  Call back Monday  "}, headers=recruiter[1])
    assert r.status_code == 201 and r.json()["body"] == "Call back Monday"
    assert db(database_url, "SELECT action FROM audit_entries")[0]["action"] == "NOTE_ADDED"
    assert client.post(f"/applications/{app['id']}/notes", json={"body": "   "}, headers=recruiter[1]).status_code == 422
    assert client.post(f"/applications/{app['id']}/notes", json={"body": "hi"}, headers=candidate[1]).status_code == 403


def test_withdraw_frees_a_slot(client, candidate, add_job, database_url):
    apps = [apply(client, candidate[1], add_job()).json() for _ in range(MAX_ACTIVE)]
    assert apply(client, candidate[1], add_job()).status_code == 422  # full
    r = client.post(f"/applications/{apps[0]['id']}/withdraw", headers=candidate[1])
    assert r.status_code == 200 and r.json()["status"] == "WITHDRAWN"
    assert apply(client, candidate[1], add_job()).status_code == 202  # room again
    assert client.post(f"/applications/{apps[0]['id']}/withdraw", headers=candidate[1]).status_code == 409


def test_decision_and_withdrawal_race_releases_the_quota_once(client, candidate, recruiter, add_job, database_url):
    """Both only change an ACTIVE application (conditional UPDATE): exactly one wins."""
    app = apply(client, candidate[1], add_job(recruiter[0])).json()
    calls = [
        lambda: client.post(f"/applications/{app['id']}/decision", json={"decision": "REJECTED", "version": 1},
                            headers=recruiter[1]).status_code,
        lambda: client.post(f"/applications/{app['id']}/withdraw", headers=candidate[1]).status_code,
    ]
    with ThreadPoolExecutor(2) as pool:
        statuses = sorted(pool.map(lambda call: call(), calls))
    assert statuses == [200, 409]
    assert db(database_url, "SELECT active_count FROM application_quotas")[0]["active_count"] == 0  # not -1
