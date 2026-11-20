"""Design v1.1 (Phase 14R, R3): several roles per account, the profile on the user, and their events."""

import time

import jwt

from tests.test_auth_flows import _create_admin, bearer, fetch_one, login, register


def claims_of(token: str) -> dict:
    return jwt.decode(token, options={"verify_signature": False})


def outbox(settings, event_type: str) -> list[dict]:
    rows = fetch_one(
        settings.database_url,
        f"SELECT coalesce(json_agg(payload->'payload' ORDER BY id), '[]') FROM outbox WHERE payload->>'event_type' = '{event_type}'",
    )[0]
    return rows


def admin_token(client, settings) -> str:
    _create_admin(settings)
    return login(client, "admin@example.com").json()["access_token"]


# ---------- adding a role to my own account ----------


def test_recruiter_adds_candidate_role_and_the_refreshed_token_has_both(client, settings):
    user_id = register(client, "rita@example.com", "RECRUITER").json()["id"]
    token = login(client, "rita@example.com").json()["access_token"]

    r = client.post("/users/me/roles", json={"role": "CANDIDATE"}, headers=bearer(token))
    assert r.status_code == 201 and r.json()["roles"] == ["CANDIDATE", "RECRUITER"]
    assert outbox(settings, "RoleGranted") == [{"user_id": user_id, "role": "CANDIDATE", "granted_by": None}]

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


def test_admin_grants_and_revokes_a_role(client, settings):
    user_id = register(client, roles=["CANDIDATE", "RECRUITER"]).json()["id"]
    user_token = login(client).json()["access_token"]
    admin = admin_token(client, settings)
    admin_id = claims_of(admin)["sub"]

    r = client.post(f"/users/{user_id}/roles", json={"role": "ADMIN"}, headers=bearer(admin))
    assert r.status_code == 201 and "ADMIN" in r.json()["roles"]
    assert outbox(settings, "RoleGranted")[-1] == {"user_id": user_id, "role": "ADMIN", "granted_by": admin_id}

    assert client.delete(f"/users/{user_id}/roles/RECRUITER", headers=bearer(admin)).status_code == 204
    assert outbox(settings, "RoleRevoked") == [{"user_id": user_id, "role": "RECRUITER", "revoked_by": admin_id}]
    tv = fetch_one(settings.database_url, f"SELECT token_version FROM users WHERE id = '{user_id}'")[0]
    assert tv == 2  # bumped: tokens that still list RECRUITER are dead
    r = client.get("/users/me", headers=bearer(user_token))
    assert r.status_code == 401

    time.sleep(1.1)  # revoked_before rejects tokens issued in the same second
    fresh = login(client).json()["access_token"]
    assert claims_of(fresh)["roles"] == ["CANDIDATE", "ADMIN"]


def test_revoke_errors(client, settings):
    user_id = register(client).json()["id"]
    user_token = login(client).json()["access_token"]
    admin = admin_token(client, settings)

    r = client.delete(f"/users/{user_id}/roles/RECRUITER", headers=bearer(admin))
    assert r.status_code == 404 and r.json()["error"]["code"] == "ROLE_NOT_HELD"
    r = client.delete(f"/users/{user_id}/roles/CANDIDATE", headers=bearer(admin))
    assert r.status_code == 409 and r.json()["error"]["code"] == "LAST_ROLE"
    assert outbox(settings, "RoleRevoked") == []  # the refused revoke rolled back completely
    assert client.get("/users/me", headers=bearer(user_token)).status_code == 200  # tokens untouched
    # Only admins grant or revoke
    assert client.post(f"/users/{user_id}/roles", json={"role": "RECRUITER"}, headers=bearer(user_token)).status_code == 403
    assert client.delete(f"/users/{user_id}/roles/CANDIDATE", headers=bearer(user_token)).status_code == 403


# ---------- PATCH /users/me ----------


def test_profile_update_emits_user_profile_updated(client, settings):
    user_id = register(client).json()["id"]
    token = login(client).json()["access_token"]

    r = client.patch("/users/me", json={"full_name": "Ana Ahmed", "current_company": "Acme"}, headers=bearer(token))
    assert r.status_code == 200
    assert (r.json()["full_name"], r.json()["current_company"]) == ("Ana Ahmed", "Acme")
    # Field names only, never the values (E.1 principle 10)
    assert outbox(settings, "UserProfileUpdated") == [{"user_id": user_id, "changed": ["current_company", "full_name"]}]

    # The same values again change nothing and emit nothing
    assert client.patch("/users/me", json={"full_name": "Ana Ahmed"}, headers=bearer(token)).status_code == 200
    assert len(outbox(settings, "UserProfileUpdated")) == 1

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
