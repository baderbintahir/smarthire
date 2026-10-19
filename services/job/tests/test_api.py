"""Steps 5.3-5.4 against the real app. Tokens come from a test key (TokenFactory), so these test
Job's own rules: roles, ownership, visibility, the state machine and optimistic locking."""

import uuid

import pytest
from smarthire_common import testing
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def job_body(**overrides) -> dict:
    body = {
        "title": "Backend Engineer",
        "description": "Build and run our APIs.",
        "location": "Remote",
        "employment_type": "FULL_TIME",
        "required_skills": ["Python", " python ", "PostgreSQL"],
        "eligibility_rules": {"min_experience_years": 2},
        "application_limit": 100,
        "stages": ["Screening", "Interview", "Offer"],
    }
    return {**body, **overrides}


@pytest.fixture
def recruiter(tokens):
    user_id = uuid.uuid4()
    return user_id, tokens.headers(user_id=user_id, role="RECRUITER")


@pytest.fixture
def other_recruiter(tokens):
    return tokens.headers(role="RECRUITER")


def create(client, headers, **overrides) -> dict:
    r = client.post("/jobs", json=job_body(**overrides), headers=headers)
    assert r.status_code == 201, r.text
    return r.json()


def force_status(database_url: str, job_id: str, status: str) -> None:
    """Stand-in for the Phase 9 workflow, which is what really moves PROCESSING -> READY."""
    async def go():
        engine = create_async_engine(database_url)
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE jobs SET status = :s WHERE id = :id"), {"s": status, "id": job_id})
        await engine.dispose()

    testing.run(go())


# ---------- create ----------


def test_recruiter_creates_a_draft(client, recruiter):
    user_id, headers = recruiter
    job = create(client, headers)
    assert job["status"] == "DRAFT" and job["version"] == 1
    assert job["recruiter_id"] == str(user_id)  # from the token, not the body
    assert job["required_skills"] == ["python", "postgresql"]  # normalized
    assert [s["name"] for s in job["stages"]] == ["Screening", "Interview", "Offer"]
    assert [s["position"] for s in job["stages"]] == [0, 1, 2]


def test_candidate_cannot_create(client, tokens):
    r = client.post("/jobs", json=job_body(), headers=tokens.headers(role="CANDIDATE"))
    assert r.status_code == 403


def test_create_requires_a_token(client):
    assert client.post("/jobs", json=job_body()).status_code == 401


@pytest.mark.parametrize(
    "overrides",
    [
        {"title": "AB"},
        {"employment_type": "FREELANCE"},
        {"stages": []},
        {"application_limit": 0},
        {"deadline": "2020-01-01T00:00:00Z"},  # in the past
        {"eligibility_rules": {"min_experience": 2}},  # typo in a rule name is rejected
    ],
)
def test_validation_errors(client, recruiter, overrides):
    assert client.post("/jobs", json=job_body(**overrides), headers=recruiter[1]).status_code == 422


def test_duplicate_stage_names_rejected(client, recruiter):
    r = client.post("/jobs", json=job_body(stages=["Screening", "screening "]), headers=recruiter[1])
    assert r.status_code == 422 and r.json()["error"]["code"] == "INVALID_STAGES"


# ---------- visibility ----------


def test_drafts_are_invisible_to_everyone_but_the_owner(client, recruiter, other_recruiter):
    job = create(client, recruiter[1])
    assert client.get(f"/jobs/{job['id']}", headers=recruiter[1]).status_code == 200
    assert client.get(f"/jobs/{job['id']}").status_code == 404  # anonymous
    assert client.get(f"/jobs/{job['id']}", headers=other_recruiter).status_code == 404
    assert client.get("/jobs").json()["items"] == []  # public board shows READY only


def test_public_board_lists_ready_jobs_with_keyset_pagination(client, recruiter, database_url):
    ids = []
    for i in range(5):
        job = create(client, recruiter[1], title=f"Job number {i}")
        force_status(database_url, job["id"], "READY")
        ids.append(job["id"])
    create(client, recruiter[1], title="Still a draft")

    page1 = client.get("/jobs?limit=2").json()
    page2 = client.get(f"/jobs?limit=2&cursor={page1['next_cursor']}").json()
    page3 = client.get(f"/jobs?limit=2&cursor={page2['next_cursor']}").json()
    seen = [j["id"] for p in (page1, page2, page3) for j in p["items"]]
    assert seen == list(reversed(ids))  # newest first, no duplicates, draft excluded
    assert page3["next_cursor"] is None


def test_my_jobs_any_status_for_recruiters_only(client, recruiter, tokens):
    create(client, recruiter[1])
    mine = client.get("/jobs?mine=true", headers=recruiter[1]).json()["items"]
    assert len(mine) == 1 and mine[0]["status"] == "DRAFT"
    assert client.get("/jobs?mine=true&status=READY", headers=recruiter[1]).json()["items"] == []
    assert client.get("/jobs?mine=true").status_code == 401
    assert client.get("/jobs?mine=true", headers=tokens.headers(role="CANDIDATE")).status_code == 403


def test_bad_token_on_public_board_is_not_anonymous(client):
    assert client.get("/jobs", headers={"Authorization": "Bearer garbage"}).status_code == 401


# ---------- edit + optimistic locking ----------


def test_owner_edits_and_version_increments(client, recruiter):
    job = create(client, recruiter[1])
    r = client.put(f"/jobs/{job['id']}", json=job_body(title="Senior Backend Engineer", stages=["Call", "Offer"], version=1),
                   headers=recruiter[1])
    assert r.status_code == 200
    assert r.json()["title"] == "Senior Backend Engineer" and r.json()["version"] == 2
    assert [s["name"] for s in r.json()["stages"]] == ["Call", "Offer"]


def test_stale_version_is_409(client, recruiter):
    job = create(client, recruiter[1])
    # Two recruiters (or two tabs) read version 1; the first save wins, the second is rejected
    assert client.put(f"/jobs/{job['id']}", json=job_body(version=1), headers=recruiter[1]).status_code == 200
    r = client.put(f"/jobs/{job['id']}", json=job_body(title="Lost update", version=1), headers=recruiter[1])
    assert r.status_code == 409 and r.json()["error"]["code"] == "VERSION_CONFLICT"
    assert client.get(f"/jobs/{job['id']}", headers=recruiter[1]).json()["title"] == "Backend Engineer"


def test_other_recruiter_gets_403_on_published_and_404_on_drafts(client, recruiter, other_recruiter, database_url):
    draft = create(client, recruiter[1])
    published = create(client, recruiter[1])
    force_status(database_url, published["id"], "READY")
    assert client.put(f"/jobs/{published['id']}", json=job_body(version=1), headers=other_recruiter).status_code == 403
    assert client.put(f"/jobs/{draft['id']}", json=job_body(version=1), headers=other_recruiter).status_code == 404
    assert client.post(f"/jobs/{published['id']}/close", headers=other_recruiter).status_code == 403


def test_editing_a_ready_job_sends_it_back_to_processing(client, recruiter, database_url):
    job = create(client, recruiter[1])
    force_status(database_url, job["id"], "READY")
    r = client.put(f"/jobs/{job['id']}", json=job_body(version=1), headers=recruiter[1])
    assert r.json()["status"] == "PROCESSING"  # content changed: must be re-processed (Phase 9)


def test_cannot_edit_while_processing(client, recruiter):
    job = create(client, recruiter[1])
    client.post(f"/jobs/{job['id']}/publish", headers=recruiter[1])
    r = client.put(f"/jobs/{job['id']}", json=job_body(version=1), headers=recruiter[1])
    assert r.status_code == 409 and r.json()["error"]["code"] == "JOB_NOT_EDITABLE"


# ---------- publish (stub) + close ----------


def test_publish_is_202_and_stays_processing(client, recruiter):
    job = create(client, recruiter[1])
    r = client.post(f"/jobs/{job['id']}/publish", headers=recruiter[1])
    assert r.status_code == 202 and r.json()["status"] == "PROCESSING"
    again = client.post(f"/jobs/{job['id']}/publish", headers=recruiter[1])
    assert again.status_code == 409 and again.json()["error"]["code"] == "INVALID_JOB_TRANSITION"


def test_close_only_ready_jobs(client, recruiter, database_url):
    job = create(client, recruiter[1])
    assert client.post(f"/jobs/{job['id']}/close", headers=recruiter[1]).status_code == 409  # DRAFT
    force_status(database_url, job["id"], "READY")
    r = client.post(f"/jobs/{job['id']}/close", headers=recruiter[1])
    assert r.status_code == 200 and r.json()["status"] == "CLOSED"
    assert client.get(f"/jobs/{job['id']}").status_code == 200  # closed jobs stay viewable
    edit = client.put(f"/jobs/{job['id']}", json=job_body(version=1), headers=recruiter[1])
    assert edit.status_code == 409  # final


def test_unknown_job_is_404(client, recruiter):
    assert client.get(f"/jobs/{uuid.uuid4()}").status_code == 404
    assert client.post(f"/jobs/{uuid.uuid4()}/publish", headers=recruiter[1]).status_code == 404


def test_ready_checks_database_and_redis(client):
    assert client.get("/health/ready").json()["checks"] == {"database": "ok", "redis": "ok"}
