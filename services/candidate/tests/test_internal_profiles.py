"""Design v2.1: the create_candidate_profile step (RegistrationWorkflow, RoleChangeWorkflow) is
PUT /internal/candidates/{id}. It replaces the UserRegistered / RoleGranted handlers of Step 7.6."""

import uuid


def put_profile(client, service_tokens, user_id, **headers):
    return client.put(f"/internal/candidates/{user_id}", headers={**service_tokens.headers(), **headers})


def test_creates_an_empty_profile_once(client, service_tokens, tokens):
    user_id = uuid.uuid4()
    assert put_profile(client, service_tokens, user_id).status_code == 200
    assert put_profile(client, service_tokens, user_id).status_code == 200  # a retried step: still one profile
    profile = client.get("/candidates/me", headers=tokens.headers(role="CANDIDATE", user_id=user_id)).json()
    assert profile["headline"] is None and profile["skills"] == []


def test_a_profile_saved_first_is_kept(client, service_tokens, tokens):
    """The candidate may save their profile before the workflow's step runs: the step must not wipe it."""
    user_id = uuid.uuid4()
    me = tokens.headers(role="CANDIDATE", user_id=user_id)
    saved = client.put("/candidates/me", json={"headline": "Jane's headline", "skills": ["python"],
                                               "experience_years": None, "education": []}, headers=me)
    assert saved.status_code == 200
    assert put_profile(client, service_tokens, user_id).status_code == 200
    assert client.get("/candidates/me", headers=me).json()["headline"] == "Jane's headline"


def test_only_the_orchestrator_may_create_profiles(client, tokens):
    user_id = uuid.uuid4()
    assert client.put(f"/internal/candidates/{user_id}", headers={"Idempotency-Key": "k"}).status_code == 401
    user = {**tokens.headers(role="ADMIN"), "Idempotency-Key": "k"}
    assert client.put(f"/internal/candidates/{user_id}", headers=user).status_code == 401
