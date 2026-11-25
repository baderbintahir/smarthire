"""Component tests: the real app against real Postgres + Redis (see conftest.py)."""

import hashlib
import time
import uuid

import jwt
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.domain.passwords import hash_password
from app.services.registration import create_user
from tests.conftest import run

COOKIE = "refresh_token"


def register(client, email="ana@example.com", role="CANDIDATE", *, roles=None, full_name="Ana Example", **extra):
    body = {"email": email, "password": "correct-horse", "full_name": full_name, "roles": roles or [role], **extra}
    return client.post("/auth/register", json=body)


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
    assert body["email"] == "ana@example.com" and body["roles"] == ["CANDIDATE"]
    assert body["full_name"] == "Ana Example" and body["current_company"] is None and body["status"] == "ACTIVE"
    assert "password" not in r.text and "hash" not in r.text
    stored = fetch_one(settings.database_url, "SELECT password_hash FROM users")[0]
    assert stored.startswith("$argon2id$")  # hashed at rest


def test_register_recruiter_with_company(client):
    r = register(client, "rita@example.com", "RECRUITER", full_name="Rita", current_company="[Company A]")
    assert r.status_code == 201
    assert (r.json()["full_name"], r.json()["current_company"], r.json()["roles"]) == ("Rita", "[Company A]", ["RECRUITER"])


def test_register_with_two_roles(client, orchestrator):
    r = register(client, "both@example.com", roles=["RECRUITER", "CANDIDATE"])
    assert r.status_code == 201 and r.json()["roles"] == ["CANDIDATE", "RECRUITER"]  # stable order (role id)
    [action] = orchestrator.inputs("register")
    assert action["user"]["roles"] == ["CANDIDATE", "RECRUITER"]
    claims = jwt.decode(login(client, "both@example.com").json()["access_token"], options={"verify_signature": False})
    assert claims["roles"] == ["CANDIDATE", "RECRUITER"] and claims["name"] == "Ana Example"


def test_registration_runs_the_register_action(client, orchestrator):
    """Design v2.1: the orchestrator's RegistrationWorkflow creates the user (its commit step is our own
    POST /internal/users). The workflow id never contains the email; the password never leaves as text."""
    import hashlib

    user_id = register(client, "ana@example.com").json()["id"]
    [(action, key, payload)] = orchestrator.actions
    assert action == "register" and key == {"email_hash": hashlib.sha256(b"ana@example.com").hexdigest()}
    assert payload["user"]["id"] == user_id and payload["user"]["password_hash"].startswith("$argon2id$")
    assert "password" not in payload["user"]


def test_duplicate_email_is_refused_before_any_action(client, orchestrator):
    register(client, "ana@example.com")
    assert register(client, "ANA@example.com").status_code == 409
    assert len(orchestrator.actions) == 1  # the duplicate never started a workflow


def test_orchestrator_down_means_503_and_no_user(client, orchestrator, settings):
    orchestrator.down = True
    r = register(client, "ana@example.com")
    assert r.status_code == 503 and r.json()["error"]["code"] == "WORKFLOWS_UNAVAILABLE"
    assert fetch_one(settings.database_url, "SELECT count(*) FROM users")[0] == 0


def test_create_user_step_is_safe_to_repeat(client, service_tokens, settings):
    """Temporal may run the commit step again after a timeout: same id -> same user, no second row."""
    user = {"id": "0190a0a0-0000-7000-8000-000000000001", "email": "rep@example.com",
            "password_hash": "$argon2id$x", "full_name": "Rep", "roles": ["CANDIDATE"]}
    first = client.post("/internal/users", json=user, headers=service_tokens.headers())
    again = client.post("/internal/users", json=user, headers=service_tokens.headers())
    assert first.status_code == again.status_code == 200 and first.json() == again.json()
    other = client.post("/internal/users", json={**user, "id": "0190a0a0-0000-7000-8000-000000000002"},
                        headers=service_tokens.headers())
    assert other.status_code == 409  # another user with the same email
    assert client.post("/internal/users", json=user).status_code == 401  # orchestrator only


def test_register_validation(client):
    assert register(client, role="ADMIN").status_code == 422  # admins can't self-register
    assert register(client, roles=["CANDIDATE", "ADMIN"]).status_code == 422  # not even alongside another role
    assert register(client, roles=["CANDIDATE", "CANDIDATE"]).status_code == 422
    assert register(client, full_name="  ").status_code == 422  # a name is required
    assert client.post("/auth/register", json={"email": "x@example.com", "password": "correct-horse",
                                               "full_name": "X", "roles": []}).status_code == 422
    r = client.post("/auth/register", json={"email": "not-an-email", "password": "short", "full_name": "X",
                                            "roles": ["CANDIDATE"]})
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
    # The cookie is "{sid}.{secret}"; Redis holds the session with only the secret's SHA-256 hash
    sid, secret = r.cookies[COOKIE].split(".")
    stored, ttl = redis_session(settings, sid)
    assert stored["secret_hash"] == hashlib.sha256(secret.encode()).hexdigest() and secret not in str(stored)
    assert stored["user_id"] == user_id_of(body["access_token"])
    assert 13 * 86400 < ttl <= settings.refresh_token_ttl_days * 86400  # 14 days
    assert fetch_one(settings.database_url, "SELECT to_regclass('identity.refresh_tokens')")[0] is None  # table gone (v1.2)


def test_token_verifies_with_published_jwks(client):
    register(client)
    token = login(client).json()["access_token"]
    jwks = client.get("/.well-known/jwks.json").json()
    kid = jwt.get_unverified_header(token)["kid"]
    jwk = next(k for k in jwks["keys"] if k["kid"] == kid)
    assert "d" not in jwk  # the private exponent must never be published
    claims = jwt.decode(token, jwt.PyJWK(jwk).key, algorithms=["RS256"], audience="smarthire")
    assert claims["roles"] == ["CANDIDATE"] and claims["name"] == "Ana Example"


def test_wrong_password_and_unknown_email_look_identical(client):
    register(client)
    wrong_pw = login(client, password="wrong-password")
    unknown = login(client, email="nobody@example.com")
    assert wrong_pw.status_code == unknown.status_code == 401
    assert wrong_pw.json()["error"]["message"] == unknown.json()["error"]["message"]
    assert wrong_pw.json()["error"]["code"] == "INVALID_CREDENTIALS"


# ---------- Step 2.10: /users/me and rate limiting ----------


def test_me_requires_valid_token(client):
    register(client, "rita@example.com", "RECRUITER", full_name="Rita")
    token = login(client, "rita@example.com").json()["access_token"]
    r = client.get("/users/me", headers=bearer(token))
    assert r.status_code == 200
    assert r.json()["email"] == "rita@example.com"
    assert r.json()["full_name"] == "Rita" and r.json()["roles"] == ["RECRUITER"]

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


def test_malformed_and_old_style_cookies_are_401(client):
    register(client)
    login(client)
    # an old Postgres-era token (no sid), garbage, and a made-up session
    for value in ("x" * 43, "a.b.c", ".", "nosuchsid.whatever"):
        assert refresh_with(client, value).status_code == 401


def test_concurrent_refreshes_with_the_same_cookie_cannot_both_win(client, settings):
    """The rotation is one Lua script: exactly one of two racing refreshes rotates; the other sees
    a stale secret, which counts as reuse and ends the session."""
    import asyncio

    from redis.asyncio import Redis

    from app.domain import tokens
    from app.repositories import sessions as sessions_repo

    register(client)
    sid, secret = login(client).cookies[COOKIE].split(".")

    async def race():
        redis = Redis.from_url(settings.redis_url, decode_responses=True)
        try:
            presented = tokens.hash_refresh_secret(secret)
            return await asyncio.gather(*(
                sessions_repo.rotate(redis, sid=sid, presented_hash=presented,
                                     new_hash=tokens.hash_refresh_secret(f"new-{i}"), ttl_seconds=60)
                for i in range(2)
            ))
        finally:
            await redis.aclose()

    statuses = sorted(status for status, _ in run(race()))
    assert statuses == [sessions_repo.REUSED, sessions_repo.ROTATED]
    assert redis_session(settings, sid)[0] == {}  # the reuse deleted the session


# ---------- Step 2.9: logout, logout-all, admin revoke ----------


def test_logout_revokes_session_and_clears_cookie(client, settings):
    register(client)
    cookie = login(client).cookies[COOKIE]
    r = client.post("/auth/logout")
    assert r.status_code == 204
    assert 'refresh_token=""' in r.headers["set-cookie"] and "Max-Age=0" in r.headers["set-cookie"]
    assert refresh_with(client, cookie).status_code == 401
    assert redis_session(settings, cookie.split(".")[0])[0] == {}  # the Redis session is gone
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
    time.sleep(1.1)  # a token issued in the same second as the revocation is rejected by design
    fresh = login(client).json()["access_token"]
    assert client.get("/users/me", headers=bearer(fresh)).status_code == 200


def _create_admin(settings, email="admin@example.com"):
    # Own engine + session: init_db()/close_db() would swap the process-wide engine that the
    # running app (inside TestClient) is using
    async def go():
        engine = create_async_engine(settings.database_url)
        async with AsyncSession(engine) as session:
            await create_user(session, id=uuid.uuid4(), email=email, password_hash=hash_password("correct-horse"),
                              full_name="Admin", current_company=None, roles=["ADMIN"])
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


def test_discovery_document_points_to_jwks(client, settings):
    r = client.get("/.well-known/openid-configuration")
    assert r.status_code == 200
    body = r.json()
    assert body["issuer"] == settings.jwt_issuer
    assert body["jwks_uri"].endswith("/.well-known/jwks.json")
    assert body["id_token_signing_alg_values_supported"] == ["RS256"]


# ---------- Phase 4.3: revocation keys for the gateway (Appendix F.3) ----------


def redis_session(settings, sid):
    async def go():
        from redis.asyncio import Redis

        r = Redis.from_url(settings.redis_url, decode_responses=True)
        try:
            return await r.hgetall(f"identity:session:{sid}"), await r.ttl(f"identity:session:{sid}")
        finally:
            await r.aclose()

    return run(go())


def redis_get(settings, key):
    async def go():
        from redis.asyncio import Redis

        r = Redis.from_url(settings.redis_url, decode_responses=True)
        try:
            return await r.get(key), await r.ttl(key)
        finally:
            await r.aclose()

    return run(go())


def user_id_of(token: str) -> str:
    return jwt.decode(token, options={"verify_signature": False})["sub"]


def test_logout_all_writes_revoked_before_for_the_gateway(client, settings):
    register(client)
    token = login(client).json()["access_token"]
    assert client.post("/auth/logout-all", headers=bearer(token)).status_code == 204
    value, ttl = redis_get(settings, f"revoked_before:{user_id_of(token)}")
    assert value is not None and int(value) >= jwt.decode(token, options={"verify_signature": False})["iat"]
    assert 0 < ttl <= settings.access_token_ttl_seconds + 60  # gone once all old tokens expired


def test_admin_suspends_user_everywhere(client, settings):
    register(client)
    victim = login(client)
    victim_token, victim_cookie = victim.json()["access_token"], victim.cookies[COOKIE]
    _create_admin(settings)
    admin_token = login(client, "admin@example.com").json()["access_token"]

    r = client.post(f"/users/{user_id_of(victim_token)}/suspend", headers=bearer(admin_token))
    assert r.status_code == 204
    assert redis_get(settings, f"revoked_before:{user_id_of(victim_token)}")[0] is not None  # gateway + services
    assert client.get("/users/me", headers=bearer(victim_token)).status_code == 401  # current token dead
    assert refresh_with(client, victim_cookie).status_code == 401  # sessions ended
    assert login(client).json()["error"]["code"] == "ACCOUNT_SUSPENDED"  # can't log back in


def test_suspend_rules(client, settings):
    register(client)
    candidate_token = login(client).json()["access_token"]
    _create_admin(settings)
    admin_token = login(client, "admin@example.com").json()["access_token"]
    admin_id = user_id_of(admin_token)

    assert client.post(f"/users/{admin_id}/suspend", headers=bearer(candidate_token)).status_code == 403
    assert client.post(f"/users/{admin_id}/suspend", headers=bearer(admin_token)).status_code == 409  # not yourself
    unknown = "01a1392c-0000-7000-8000-000000000000"
    assert client.post(f"/users/{unknown}/suspend", headers=bearer(admin_token)).status_code == 404


def test_internal_contact_for_other_services(client):
    user_id = register(client, "ana@example.com").json()["id"]
    r = client.get(f"/internal/users/{user_id}/contact")
    assert r.status_code == 200 and r.json() == {"user_id": user_id, "email": "ana@example.com",
                                                 "roles": ["CANDIDATE"], "status": "ACTIVE"}
    assert client.get("/internal/users/01a1b2c3-0000-7000-8000-000000000009/contact").status_code == 404
