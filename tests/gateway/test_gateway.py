"""Gateway (APISIX) checks against the RUNNING stack: `make up`, then `make test-gateway`.

These test configuration, not code: routes, auth at the edge, revocation, rate limits, error
shapes. Tests that prove a request was stopped AT THE GATEWAY send a unique X-Request-ID and
check that Identity never logged it."""

import subprocess
import time
import uuid

import httpx
import jwt
import pytest

GATEWAY = "http://localhost:8000"
ID = f"{GATEWAY}/api/identity"


@pytest.fixture(scope="session", autouse=True)
def gateway_running():
    try:
        httpx.get(f"{ID}/.well-known/jwks.json", timeout=3).raise_for_status()
    except httpx.HTTPError:
        pytest.skip("Stack not running: `make up` first")


def new_user(role="CANDIDATE") -> tuple[str, str]:
    """Registers a fresh user through the gateway; returns (email, access_token)."""
    email = f"gw-{uuid.uuid4().hex[:10]}@example.com"
    extra = {"display_name": "Rita"} if role == "RECRUITER" else {}
    r = httpx.post(f"{ID}/auth/register", json={"email": email, "password": "correct-horse", "role": role, **extra})
    assert r.status_code == 201, r.text
    return email, login(email)


def login(email: str) -> str:
    r = httpx.post(f"{ID}/auth/login", json={"email": email, "password": "correct-horse"})
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


def new_admin() -> str:
    """Admins can't self-register: created with the CLI inside the Identity container."""
    email = f"gw-admin-{uuid.uuid4().hex[:10]}@example.com"
    subprocess.run(
        ["docker", "compose", "exec", "-T", "identity", "python", "-m", "app.cli", "create-admin", email],
        input="correct-horse\n", text=True, check=True, capture_output=True,
    )
    return login(email)


def bearer(token: str, request_id: str | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    if request_id:
        headers["X-Request-ID"] = request_id
    return headers


def identity_saw(request_id: str) -> bool:
    time.sleep(0.5)  # let the log line land
    logs = subprocess.run(["docker", "compose", "logs", "identity", "--since", "2m"], capture_output=True, text=True)
    return request_id in logs.stdout


def claims(token: str) -> dict:
    return jwt.decode(token, options={"verify_signature": False})


# ---------- 4.2 routing, request IDs, error shapes ----------


def test_unknown_path_is_json_404():
    r = httpx.get(f"{GATEWAY}/definitely/not/a/route")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "NOT_FOUND"


def test_service_not_deployed_is_json_503():
    r = httpx.get(f"{GATEWAY}/api/jobs")  # public route; there is no job service yet
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "SERVICE_UNAVAILABLE"


def test_public_routes_need_no_token():
    assert httpx.get(f"{ID}/.well-known/jwks.json").status_code == 200
    assert httpx.get(f"{ID}/.well-known/openid-configuration").status_code == 200


def test_request_id_reused_or_generated_and_reaches_the_service():
    _, token = new_user()
    rid = f"gw-test-{uuid.uuid4()}"
    r = httpx.get(f"{ID}/users/me", headers=bearer(token, rid))
    assert r.headers["X-Request-ID"] == rid
    assert identity_saw(rid)  # same id in the service's logs
    assert httpx.get(f"{ID}/.well-known/jwks.json").headers["X-Request-ID"]  # generated when absent


# ---------- 4.3 authentication and revocation at the edge ----------


def test_protected_route_rejects_missing_and_tampered_tokens_at_the_gateway():
    _, token = new_user()
    rid = f"gw-test-{uuid.uuid4()}"
    r = httpx.get(f"{ID}/users/me", headers={"X-Request-ID": rid})
    assert r.status_code == 401 and r.json()["error"]["code"] == "INVALID_TOKEN"
    tampered = token[:-4] + ("AAAA" if not token.endswith("AAAA") else "BBBB")
    assert httpx.get(f"{ID}/users/me", headers=bearer(tampered, rid)).status_code == 401
    assert not identity_saw(rid)  # stopped at the edge
    assert httpx.get(f"{ID}/users/me", headers=bearer(token)).status_code == 200


def test_admin_revoked_token_is_blocked_at_the_gateway():
    _, victim = new_user()
    admin = new_admin()
    assert httpx.post(f"{ID}/auth/revoke/{claims(victim)['jti']}", headers=bearer(admin)).status_code == 204

    rid = f"gw-test-{uuid.uuid4()}"
    r = httpx.get(f"{ID}/users/me", headers=bearer(victim, rid))
    assert r.status_code == 401 and r.json()["error"]["message"] == "Token has been revoked"
    assert not identity_saw(rid)  # Identity never saw the request


def test_logout_everywhere_blocks_old_tokens_at_the_gateway():
    email, old = new_user()
    assert httpx.post(f"{ID}/auth/logout-all", headers=bearer(old)).status_code == 204
    rid = f"gw-test-{uuid.uuid4()}"
    assert httpx.get(f"{ID}/users/me", headers=bearer(old, rid)).status_code == 401
    assert not identity_saw(rid)
    time.sleep(1.1)  # tokens issued in the same second as the revocation are allowed by design
    assert httpx.get(f"{ID}/users/me", headers=bearer(login(email))).status_code == 200


def test_suspended_user_is_blocked_at_the_gateway_immediately():
    _, victim = new_user()
    admin = new_admin()
    assert httpx.post(f"{ID}/users/{claims(victim)['sub']}/suspend", headers=bearer(admin)).status_code == 204
    rid = f"gw-test-{uuid.uuid4()}"
    assert httpx.get(f"{ID}/users/me", headers=bearer(victim, rid)).status_code == 401
    assert not identity_saw(rid)


# ---------- 4.4 rate limiting ----------


def test_per_user_rate_limit_returns_429_with_retry_after():
    _, token = new_user()  # a fresh user: other tests' traffic doesn't count against it
    with httpx.Client(headers=bearer(token)) as client:
        statuses = [client.get(f"{ID}/users/me").status_code for _ in range(301)]
        assert statuses.count(429) >= 1 and statuses[:300].count(200) >= 299
        r = client.get(f"{ID}/users/me")
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "TOO_MANY_REQUESTS"
    assert r.headers["Retry-After"].isdigit() and int(r.headers["Retry-After"]) > 0
