"""Steps 6.2-6.3 against the real app (Postgres + Redis + MinIO). Tokens come from a test key
(TokenFactory), so these test Candidate's own rules: roles, own-profile-only, file checks, and
the one-active-resume switch."""

import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from smarthire_common import testing
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.models.candidate import DOCX, PDF
from tests.test_domain import MINIMAL_DOCX, MINIMAL_PDF

PROFILE = {
    "full_name": "Jane Doe",
    "headline": "Backend developer",
    "skills": ["Python", " python ", "PostgreSQL"],
    "experience_years": 3.5,
    "education": [{"institution": "Example University", "degree": "BSc", "field": "CS",
                   "start_year": 2015, "end_year": 2019}],
}


@pytest.fixture
def candidate(tokens):
    user_id = uuid.uuid4()
    return user_id, tokens.headers(user_id=user_id, role="CANDIDATE")


@pytest.fixture
def with_profile(client, candidate):
    user_id, headers = candidate
    assert client.put("/candidates/me", json=PROFILE, headers=headers).status_code == 200
    return user_id, headers


def upload(client, headers, data=MINIMAL_PDF, name="cv.pdf", content_type="application/pdf"):
    return client.post("/candidates/me/resumes", files={"file": (name, data, content_type)}, headers=headers)


def db_rows(database_url, sql):
    async def go():
        engine = create_async_engine(database_url)
        async with engine.connect() as conn:
            rows = (await conn.execute(text(sql))).mappings().all()
        await engine.dispose()
        return rows

    return testing.run(go())


# ---------- profile (6.2) ----------


def test_no_profile_yet_is_404(client, candidate):
    r = client.get("/candidates/me", headers=candidate[1])
    assert r.status_code == 404 and r.json()["error"]["code"] == "CANDIDATE_NOT_FOUND"


def test_put_creates_then_updates_the_profile(client, candidate):
    user_id, headers = candidate
    r = client.put("/candidates/me", json=PROFILE, headers=headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["id"] == str(user_id)  # the profile id IS the Identity user id
    assert body["skills"] == ["python", "postgresql"]  # normalized
    assert body["experience_years"] == 3.5 and body["active_resume"] is None

    r = client.put("/candidates/me", json={"full_name": "Jane Q. Doe"}, headers=headers)
    assert r.json()["full_name"] == "Jane Q. Doe"
    assert r.json()["headline"] is None and r.json()["skills"] == []  # PUT replaces the whole profile
    assert client.get("/candidates/me", headers=headers).json()["full_name"] == "Jane Q. Doe"


def test_concurrent_first_saves_both_succeed(client, candidate):
    """Upsert in one statement: no "both saw no row, second insert fails" race."""
    _, headers = candidate
    with ThreadPoolExecutor(5) as pool:
        statuses = list(pool.map(lambda _: client.put("/candidates/me", json=PROFILE, headers=headers).status_code,
                                 range(5)))
    assert statuses == [200] * 5


@pytest.mark.parametrize(
    "bad",
    [
        {"full_name": ""},
        {"full_name": "Jane", "experience_years": -1},
        {"full_name": "Jane", "experience_years": 2.25},  # one decimal place, like numeric(4,1)
        {"full_name": "Jane", "education": [{"institution": "U", "degree": "BSc", "grade": "A"}]},  # unknown key
        {"full_name": "Jane", "education": [{"institution": "U", "degree": "BSc",
                                             "start_year": 2020, "end_year": 2019}]},
    ],
)
def test_invalid_profiles_are_422(client, candidate, bad):
    assert client.put("/candidates/me", json=bad, headers=candidate[1]).status_code == 422


def test_recruiters_cannot_use_the_me_endpoints(client, tokens):
    headers = tokens.headers(role="RECRUITER")
    assert client.get("/candidates/me", headers=headers).status_code == 403
    assert client.put("/candidates/me", json=PROFILE, headers=headers).status_code == 403
    assert upload(client, headers).status_code == 403


def test_no_token_is_401(client):
    assert client.get("/candidates/me").status_code == 401
    assert client.post("/candidates/me/resumes", files={"file": ("cv.pdf", MINIMAL_PDF)}).status_code == 401


def test_reading_a_profile_by_id(client, with_profile, tokens):
    user_id, own_headers = with_profile
    assert client.get(f"/candidates/{user_id}", headers=own_headers).status_code == 200  # myself
    # Recruiters may read profiles for now (Phase 8 tightens this to their own applicants)
    assert client.get(f"/candidates/{user_id}", headers=tokens.headers(role="RECRUITER")).status_code == 200
    # Another candidate: 403, for existing and unknown ids alike (no probing which ids exist)
    other = tokens.headers(role="CANDIDATE")
    assert client.get(f"/candidates/{user_id}", headers=other).status_code == 403
    assert client.get(f"/candidates/{uuid.uuid4()}", headers=other).status_code == 403
    assert client.get(f"/candidates/{uuid.uuid4()}", headers=tokens.headers(role="RECRUITER")).status_code == 404


# ---------- resume upload (6.3) ----------


def test_upload_needs_a_profile_first(client, candidate, bucket_objects):
    r = upload(client, candidate[1])
    assert r.status_code == 404 and "PUT /candidates/me" in r.json()["error"]["message"]
    assert bucket_objects() == []  # checked before anything was stored


def test_upload_stores_the_file_and_the_row(client, with_profile, bucket_objects, database_url):
    user_id, headers = with_profile
    r = upload(client, headers, name="Jane CV.pdf")
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["status"] == "UPLOADED" and body["is_active"] and body["content_type"] == PDF
    assert body["file_name"] == "Jane CV.pdf" and body["size_bytes"] == len(MINIMAL_PDF)
    assert "object_key" not in body  # storage layout stays internal

    key = f"{user_id}/{body['id']}"
    assert bucket_objects() == [key]  # resumes/{candidate_id}/{resume_id}
    [row] = db_rows(database_url, "SELECT object_key, is_active FROM resumes")
    assert row["object_key"] == key and row["is_active"]
    assert client.get("/candidates/me", headers=headers).json()["active_resume"]["id"] == body["id"]


def test_docx_is_accepted(client, with_profile):
    r = upload(client, with_profile[1], data=MINIMAL_DOCX, name="cv.docx", content_type=DOCX)
    assert r.status_code == 202 and r.json()["content_type"] == DOCX


def test_the_type_comes_from_the_bytes_not_the_header(client, with_profile, bucket_objects):
    _, headers = with_profile
    # An executable that CLAIMS to be a PDF: name and Content-Type both lie
    r = upload(client, headers, data=b"MZ\x90\x00 not a pdf", name="cv.pdf", content_type="application/pdf")
    assert r.status_code == 415 and r.json()["error"]["code"] == "UNSUPPORTED_FILE_TYPE"
    # A real PDF sent with a wrong header is still a PDF
    r = upload(client, headers, content_type="text/plain")
    assert r.status_code == 202 and r.json()["content_type"] == PDF
    assert len(bucket_objects()) == 1


def test_too_large_is_413_and_nothing_is_stored(client, with_profile, bucket_objects, settings):
    data = MINIMAL_PDF + b"0" * settings.max_resume_bytes  # just over 5 MB
    r = upload(client, with_profile[1], data=data)
    assert r.status_code == 413 and r.json()["error"]["code"] == "FILE_TOO_LARGE"
    assert bucket_objects() == []


def test_empty_file_and_missing_field_are_rejected(client, with_profile):
    _, headers = with_profile
    assert upload(client, headers, data=b"").status_code == 415
    assert client.post("/candidates/me/resumes", headers=headers).status_code == 422  # no "file" field


def test_new_upload_replaces_the_active_resume(client, with_profile, bucket_objects):
    _, headers = with_profile
    first = upload(client, headers).json()
    second = upload(client, headers, data=MINIMAL_DOCX, name="cv.docx").json()
    items = client.get("/candidates/me/resumes", headers=headers).json()["items"]
    assert [(r["id"], r["is_active"]) for r in items] == [(second["id"], True), (first["id"], False)]  # newest first
    assert len(bucket_objects()) == 2  # old files are kept (history); only the flag changes


def test_concurrent_uploads_leave_exactly_one_active(client, with_profile, database_url):
    """The candidate row lock makes uploads take turns: all succeed, one ends up active."""
    _, headers = with_profile
    with ThreadPoolExecutor(8) as pool:
        statuses = list(pool.map(lambda _: upload(client, headers).status_code, range(8)))
    assert statuses == [202] * 8
    [row] = db_rows(database_url, "SELECT count(*) FILTER (WHERE is_active) AS active, count(*) AS total FROM resumes")
    assert (row["active"], row["total"]) == (1, 8)


def test_failed_row_insert_removes_the_uploaded_file(client, with_profile, bucket_objects, monkeypatch):
    """File first, row second: if the row can't be written, the file is deleted again."""
    from app.repositories import candidates as repo

    async def broken(*args, **kwargs):
        raise RuntimeError("database went away")

    monkeypatch.setattr(repo, "add_active_resume", broken)
    with pytest.raises(RuntimeError):  # TestClient re-raises server errors by default
        upload(client, with_profile[1])
    assert bucket_objects() == []


def test_readiness_includes_storage(client):
    r = client.get("/health/ready")
    assert r.status_code == 200, r.text
    assert "storage" in r.text
