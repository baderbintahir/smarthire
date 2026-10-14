"""Component tests: the real app against real Postgres + Redis (see conftest.py)."""

import jwt
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.services.registration import register_user
from tests.conftest import run

COOKIE = "refresh_token"


def register(client, email="ana@example.com", role="CANDIDATE", **extra):
    return client.post("/auth/register", json={"email": email, "password": "correct-horse", "role": role, **extra})


def login(client, email="ana@example.com", password="correct-horse"):
    return client.post("/auth/login", json={"email": email, "password": password})


def bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def refresh_with(client, cookie_value: str):
    """Send a specific refresh cookie (e.g. an old one), ignoring the client's cookie jar."""
    client.cookies.clear()
    return client.post("/auth/refresh", headers={"Cookie": f"{COOKIE}={cookie_value}"})


def fetch_one(database_url: str, sql: str):
    async def go():
        engine = create_async_engine(database_url)
        async with engine.connect() as conn:
            row = (await conn.execute(text(sql))).one()
        await engine.dispose()
        return row

    return run(go())


# ---------- Step 2.4: register ----------


def test_register_candidate(client, settings):
    r = register(client)
    assert r.status_code == 201
    body = r.json()
    assert body["email"] == "ana@example.com" and body["role"] == "CANDIDATE"
    assert body["status"] == "ACTIVE" and body["recruiter_profile"] is None
    assert "password" not in r.text and "hash" not in r.text
    stored = fetch_one(settings.database_url, "SELECT password_hash FROM users")[0]
    assert stored.startswith("$argon2id$")  # hashed at rest


def test_register_recruiter_creates_profile(client):
    r = register(client, "rita@example.com", "RECRUITER", display_name="Rita", company="[Company A]")
    assert r.status_code == 201
    assert r.json()["recruiter_profile"] == {"display_name": "Rita", "company": "[Company A]"}


def test_register_validation(client):
    assert register(client, role="RECRUITER").status_code == 422  # recruiter needs display_name
    assert register(client, role="ADMIN").status_code == 422  # admins can't self-register
    r = client.post("/auth/register", json={"email": "not-an-email", "password": "short", "role": "CANDIDATE"})
    assert r.status_code == 422 and r.json()["error"]["code"] == "VALIDATION_ERROR"


def test_duplicate_email_is_409_case_insensitive(client):
    assert register(client, "ana@example.com").status_code == 201
    r = register(client, "ANA@Example.com")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "EMAIL_ALREADY_REGISTERED"


# ---------- Steps 2.6 / 2.7: JWKS and login ----------


def test_login_returns_token_and_secure_cookie(client, settings):
    register(client)
    r = login(client)
    assert r.status_code == 200
    body = r.json()
    assert body["token_type"] == "Bearer" and body["expires_in"] == 900
    cookie_header = r.headers["set-cookie"]
    for flag in ("HttpOnly", "Secure", "SameSite=strict", "Path=/auth"):
        assert flag in cookie_header
    # Only the SHA-256 hash is stored, never the token itself
    stored = fetch_one(settings.database_url, "SELECT token_hash FROM refresh_tokens")[0]
    assert len(stored) == 32 and r.cookies[COOKIE].encode() not in bytes(stored)


def test_token_verifies_with_published_jwks(client):
    register(client)
    token = login(client).json()["access_token"]
    jwks = client.get("/.well-known/jwks.json").json()
    kid = jwt.get_unverified_header(token)["kid"]
    jwk = next(k for k in jwks["keys"] if k["kid"] == kid)
    assert "d" not in jwk  # the private exponent must never be published
    claims = jwt.decode(token, jwt.PyJWK(jwk).key, algorithms=["RS256"], audience="smarthire")
    assert claims["role"] == "CANDIDATE"


def test_wrong_password_and_unknown_email_look_identical(client):
    register(client)
    wrong_pw = login(client, password="wrong-password")
    unknown = login(client, email="nobody@example.com")
    assert wrong_pw.status_code == unknown.status_code == 401
    assert wrong_pw.json()["error"]["message"] == unknown.json()["error"]["message"]
    assert wrong_pw.json()["error"]["code"] == "INVALID_CREDENTIALS"


# ---------- Step 2.10: /users/me and rate limiting ----------


def test_me_requires_valid_token(client):
    register(client, "rita@example.com", "RECRUITER", display_name="Rita")
    token = login(client, "rita@example.com").json()["access_token"]
    r = client.get("/users/me", headers=bearer(token))
    assert r.status_code == 200
    assert r.json()["email"] == "rita@example.com"
    assert r.json()["recruiter_profile"]["display_name"] == "Rita"

    missing = client.get("/users/me")
    assert missing.status_code == 401 and missing.headers["WWW-Authenticate"] == "Bearer"
    tampered = token[:-4] + ("AAAA" if not token.endswith("AAAA") else "BBBB")
    assert client.get("/users/me", headers=bearer(tampered)).status_code == 401


def test_failed_logins_are_rate_limited_per_email(client):
    register(client)
    for _ in range(3):  # limit is 3 in the test settings
        assert login(client, password="wrong-password").status_code == 401
    r = login(client)  # even the CORRECT password is refused while blocked
    assert r.status_code == 429
    assert int(r.headers["Retry-After"]) > 0


def test_successful_login_resets_email_counter(client):
    register(client)
    login(client, password="wrong-password")
    login(client, password="wrong-password")
    assert login(client).status_code == 200  # 2 failures < 3: allowed, and resets the counter
    login(client, password="wrong-password")
    login(client, password="wrong-password")
    assert login(client).status_code == 200


def test_failed_logins_are_rate_limited_per_ip(client):
    for i in range(6):  # 6 different accounts from one IP (limit is 6)
        login(client, email=f"victim{i}@example.com", password="guess")
    assert login(client, email="someone-new@example.com", password="guess").status_code == 429


# ---------- Step 2.8: refresh rotation and reuse detection ----------


def test_refresh_rotates_the_token(client):
    register(client)
    first = login(client).cookies[COOKIE]
    r = refresh_with(client, first)
    assert r.status_code == 200
    second = r.cookies[COOKIE]
    assert second != first
    assert client.get("/users/me", headers=bearer(r.json()["access_token"])).status_code == 200
    assert refresh_with(client, second).status_code == 200  # the new one works


def test_reusing_an_old_refresh_token_revokes_the_whole_session(client):
    register(client)
    old = login(client).cookies[COOKIE]
    new = refresh_with(client, old).cookies[COOKIE]

    reuse = refresh_with(client, old)  # an attacker (or the user) replays the rotated token
    assert reuse.status_code == 401
    assert reuse.json()["error"]["code"] == "INVALID_REFRESH_TOKEN"
    # The legitimate latest token is dead too: the whole family was revoked -> must log in again
    assert refresh_with(client, new).status_code == 401
    assert login(client).status_code == 200


def test_refresh_without_cookie_is_401(client):
    client.cookies.clear()
    assert client.post("/auth/refresh").status_code == 401


# ---------- Step 2.9: logout, logout-all, admin revoke ----------


def test_logout_revokes_session_and_clears_cookie(client):
    register(client)
    cookie = login(client).cookies[COOKIE]
    r = client.post("/auth/logout")
    assert r.status_code == 204
    assert 'refresh_token=""' in r.headers["set-cookie"] and "Max-Age=0" in r.headers["set-cookie"]
    assert refresh_with(client, cookie).status_code == 401
    assert client.post("/auth/logout").status_code == 204  # idempotent


def test_logout_all_kills_every_session_and_current_access_tokens(client):
    register(client)
    laptop = login(client)
    phone = login(client)
    access = laptop.json()["access_token"]

    assert client.post("/auth/logout-all", headers=bearer(access)).status_code == 204
    assert refresh_with(client, laptop.cookies[COOKIE]).status_code == 401
    assert refresh_with(client, phone.cookies[COOKIE]).status_code == 401
    # token_version was bumped, so Identity rejects the old access token immediately
    assert client.get("/users/me", headers=bearer(access)).status_code == 401
    fresh = login(client).json()["access_token"]
    assert client.get("/users/me", headers=bearer(fresh)).status_code == 200


def _create_admin(settings, email="admin@example.com"):
    # Own engine + session: init_db()/close_db() would swap the process-wide engine that the
    # running app (inside TestClient) is using
    async def go():
        engine = create_async_engine(settings.database_url)
        async with AsyncSession(engine) as session:
            await register_user(session, email=email, password="correct-horse", role="ADMIN")
        await engine.dispose()

    run(go())


def test_admin_can_revoke_a_specific_access_token(client, settings):
    register(client)
    victim_token = login(client).json()["access_token"]
    _create_admin(settings)
    admin_token = login(client, "admin@example.com").json()["access_token"]

    jti = jwt.decode(victim_token, options={"verify_signature": False})["jti"]
    assert client.post(f"/auth/revoke/{jti}", headers=bearer(admin_token)).status_code == 204
    r = client.get("/users/me", headers=bearer(victim_token))
    assert r.status_code == 401 and r.json()["error"]["message"] == "Token has been revoked"
    assert client.get("/users/me", headers=bearer(admin_token)).status_code == 200  # others unaffected


def test_non_admin_cannot_revoke(client):
    register(client)
    token = login(client).json()["access_token"]
    r = client.post("/auth/revoke/some-jti", headers=bearer(token))
    assert r.status_code == 403  # valid token, wrong role: 403 not 401
    assert client.post("/auth/revoke/some-jti").status_code == 401  # no token at all: 401


# ---------- health ----------


def test_ready_checks_database_and_redis(client):
    r = client.get("/health/ready")
    assert r.status_code == 200
    assert r.json()["checks"] == {"database": "ok", "redis": "ok"}
