"""Design v1.1 (Phase 14R, R3): several roles per account and the profile on the user. Design v2.1: role
changes run as the orchestrator's "grant-role" / "revoke-role" actions (a fake orchestrator here)."""

import time

import jwt

from tests.test_auth_flows import _create_admin, bearer, fetch_one, login, register


def claims_of(token: str) -> dict:
    return jwt.decode(token, options={"verify_signature": False})


def admin_token(client, settings) -> str:
    _create_admin(settings)
    return login(client, "admin@example.com").json()["access_token"]


# ---------- adding a role to my own account ----------


def test_recruiter_adds_candidate_role_and_the_refreshed_token_has_both(client, orchestrator):
    user_id = register(client, "rita@example.com", "RECRUITER").json()["id"]
    token = login(client, "rita@example.com").json()["access_token"]

    r = client.post("/users/me/roles", json={"role": "CANDIDATE"}, headers=bearer(token))
    assert r.status_code == 201 and r.json()["roles"] == ["CANDIDATE", "RECRUITER"]
    assert orchestrator.inputs("grant-role") == [{"user_id": user_id, "role": "CANDIDATE", "granted_by": None}]

    assert claims_of(token)["roles"] == ["RECRUITER"]  # the current token is unchanged...
    refreshed = client.post("/auth/refresh").json()["access_token"]  # ...the next one has both
    assert claims_of(refreshed)["roles"] == ["CANDIDATE", "RECRUITER"]


def test_adding_a_role_twice_is_409(client):
    register(client)
    token = login(client).json()["access_token"]
    r = client.post("/users/me/roles", json={"role": "CANDIDATE"}, headers=bearer(token))
    assert r.status_code == 409 and r.json()["error"]["code"] == "ROLE_ALREADY_HELD"


def test_nobody_gives_themselves_admin(client):
    register(client)
    token = login(client).json()["access_token"]
    assert client.post("/users/me/roles", json={"role": "ADMIN"}, headers=bearer(token)).status_code == 422


# ---------- admin grant and revoke ----------


def test_admin_grants_and_revokes_a_role(client, settings, orchestrator):
    user_id = register(client, roles=["CANDIDATE", "RECRUITER"]).json()["id"]
    user_token = login(client).json()["access_token"]
    admin = admin_token(client, settings)
    admin_id = claims_of(admin)["sub"]

    r = client.post(f"/users/{user_id}/roles", json={"role": "ADMIN"}, headers=bearer(admin))
    assert r.status_code == 201 and "ADMIN" in r.json()["roles"]
    assert orchestrator.inputs("grant-role")[-1] == {"user_id": user_id, "role": "ADMIN", "granted_by": admin_id}

    assert client.delete(f"/users/{user_id}/roles/RECRUITER", headers=bearer(admin)).status_code == 204
    assert orchestrator.inputs("revoke-role") == [{"user_id": user_id, "role": "RECRUITER", "revoked_by": admin_id}]
    tv = fetch_one(settings.database_url, f"SELECT token_version FROM users WHERE id = '{user_id}'")[0]
    assert tv == 2  # bumped: tokens that still list RECRUITER are dead
    r = client.get("/users/me", headers=bearer(user_token))
    assert r.status_code == 401

    time.sleep(1.1)  # revoked_before rejects tokens issued in the same second
    fresh = login(client).json()["access_token"]
    assert claims_of(fresh)["roles"] == ["CANDIDATE", "ADMIN"]


def test_revoke_errors(client, settings, orchestrator):
    user_id = register(client).json()["id"]
    user_token = login(client).json()["access_token"]
    admin = admin_token(client, settings)

    r = client.delete(f"/users/{user_id}/roles/RECRUITER", headers=bearer(admin))
    assert r.status_code == 404 and r.json()["error"]["code"] == "ROLE_NOT_HELD"
    r = client.delete(f"/users/{user_id}/roles/CANDIDATE", headers=bearer(admin))
    assert r.status_code == 409 and r.json()["error"]["code"] == "LAST_ROLE"
    assert orchestrator.inputs("revoke-role") == []  # refused before any workflow started
    assert client.get("/users/me", headers=bearer(user_token)).status_code == 200  # tokens untouched
    # Only admins grant or revoke
    assert client.post(f"/users/{user_id}/roles", json={"role": "RECRUITER"}, headers=bearer(user_token)).status_code == 403
    assert client.delete(f"/users/{user_id}/roles/CANDIDATE", headers=bearer(user_token)).status_code == 403


# ---------- PATCH /users/me ----------


def test_profile_update_is_a_plain_write(client, orchestrator):
    register(client)
    token = login(client).json()["access_token"]

    r = client.patch("/users/me", json={"full_name": "Ana Ahmed", "current_company": "Acme"}, headers=bearer(token))
    assert r.status_code == 200
    assert (r.json()["full_name"], r.json()["current_company"]) == ("Ana Ahmed", "Acme")
    # No other service reacts to a profile edit: no workflow (only the registration's action)
    assert [a for a, _, _ in orchestrator.actions] == ["register"]
    assert client.patch("/users/me", json={"full_name": "Ana Ahmed"}, headers=bearer(token)).status_code == 200

    # null clears the employer; the name can't be null
    r = client.patch("/users/me", json={"current_company": None}, headers=bearer(token))
    assert r.status_code == 200 and r.json()["current_company"] is None
    assert client.patch("/users/me", json={"full_name": None}, headers=bearer(token)).status_code == 422
    assert client.patch("/users/me", json={}, headers=bearer(token)).status_code == 422

    assert claims_of(client.post("/auth/refresh").json()["access_token"])["name"] == "Ana Ahmed"


# ---------- internal contact API ----------


def test_internal_contact_lists_roles(client):
    user_id = register(client, roles=["RECRUITER", "CANDIDATE"]).json()["id"]
    r = client.get(f"/internal/users/{user_id}/contact")
    assert r.json() == {"user_id": user_id, "email": "ana@example.com", "roles": ["CANDIDATE", "RECRUITER"], "status": "ACTIVE"}


def test_role_steps_are_safe_to_repeat(client, settings, service_tokens):
    """Temporal may run a commit step twice: granting a held role or revoking a gone one changes nothing."""
    user_id = register(client, roles=["CANDIDATE", "RECRUITER"]).json()["id"]
    grant = {"role": "ADMIN", "granted_by": None}
    for _ in range(2):
        r = client.post(f"/internal/users/{user_id}/roles", json=grant, headers=service_tokens.headers())
        assert r.status_code == 200 and "ADMIN" in r.json()["roles"]
    for _ in range(2):
        r = client.delete(f"/internal/users/{user_id}/roles/RECRUITER", headers=service_tokens.headers())
        assert r.status_code == 204
    tv = fetch_one(settings.database_url, f"SELECT token_version FROM users WHERE id = '{user_id}'")[0]
    assert tv == 2  # bumped once, not twice
