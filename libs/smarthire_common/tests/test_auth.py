import uuid
from datetime import UTC, datetime, timedelta

import fakeredis
import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm
from redis.exceptions import ConnectionError as RedisConnectionError

from smarthire_common import auth
from smarthire_common.auth import (
    CurrentUser,
    JWKSClient,
    StaticKeyProvider,
    ensure_owner,
    get_current_user,
    require_role,
)
from smarthire_common.errors import (
    ForbiddenError,
    ServiceUnavailableError,
    UnauthorizedError,
    register_exception_handlers,
)

ISS, AUD = "smarthire-identity", "smarthire"


def new_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


KEY_A, KEY_B = new_key(), new_key()


def make_token(private_key=KEY_A, kid="a", **overrides):
    now = datetime.now(UTC)
    claims = {
        "sub": str(uuid.uuid4()), "role": "CANDIDATE", "iss": ISS, "aud": AUD, "jti": str(uuid.uuid4()),
        "iat": int(now.timestamp()), "exp": int((now + timedelta(minutes=15)).timestamp()),
    }
    claims.update(overrides)
    return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": kid})


def jwks_body(*pairs):
    return {"keys": [{**RSAAlgorithm.to_jwk(k.public_key(), as_dict=True), "kid": kid} for kid, k in pairs]}


class FakeIdentity:
    """Stands in for GET /.well-known/jwks.json and counts calls. No network involved."""

    def __init__(self, *pairs):
        self.body, self.calls, self.fail = jwks_body(*pairs), 0, False

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        return httpx.Response(503) if self.fail else httpx.Response(200, json=self.body)

    def client(self, clock):
        http = httpx.AsyncClient(transport=httpx.MockTransport(self.handler))
        return JWKSClient("http://identity/jwks", http_client=http, clock=clock)


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture
def redis(monkeypatch):
    fake = fakeredis.FakeAsyncRedis(decode_responses=True)
    monkeypatch.setattr(auth, "get_redis", lambda: fake)
    return fake


@pytest.fixture
def static_auth(redis):
    auth.init_auth(StaticKeyProvider({"a": KEY_A.public_key()}), issuer=ISS, audience=AUD)
    yield
    auth._config = None


# ---------- verify_token ----------


async def test_valid_token(static_auth):
    claims = await auth.verify_token(make_token(role="RECRUITER"))
    assert claims["role"] == "RECRUITER"


@pytest.mark.parametrize(
    "token",
    [
        make_token(exp=int((datetime.now(UTC) - timedelta(hours=1)).timestamp())),  # expired
        make_token(iss="evil"),  # wrong issuer
        make_token(aud="other-system"),  # wrong audience
        make_token(private_key=KEY_B),  # signed by someone else's key
        make_token(kid="unknown"),  # key we don't know
        jwt.encode({"sub": "x"}, None, algorithm="none"),  # unsigned
        "not-a-jwt",
    ],
)
async def test_bad_tokens_are_401(static_auth, token):
    with pytest.raises(UnauthorizedError):
        await auth.verify_token(token)


async def test_tampered_token_is_401(static_auth):
    header, payload, sig = make_token().split(".")
    tampered = f"{header}.{payload[:-2]}{'A' if payload[-2] != 'A' else 'B'}{payload[-1]}.{sig}"
    with pytest.raises(UnauthorizedError):
        await auth.verify_token(tampered)


async def test_denylisted_token_is_401(static_auth, redis):
    token = make_token(jti="revoked-1")
    await redis.set(auth.denylist_key("revoked-1"), "1")
    with pytest.raises(UnauthorizedError, match="revoked"):
        await auth.verify_token(token)


async def test_tokens_issued_before_revoked_before_are_401(static_auth, redis):
    sub = str(uuid.uuid4())
    now = int(datetime.now(UTC).timestamp())
    old = make_token(sub=sub, iat=now - 60)  # issued a minute ago
    same_second = make_token(sub=sub, iat=now)  # can't be told apart from "just before": rejected
    newer = make_token(sub=sub, iat=now + 1)  # issued after the revocation (logged in again)
    await redis.set(auth.revoked_before_key(sub), str(now))
    for token in (old, same_second):
        with pytest.raises(UnauthorizedError, match="revoked"):
            await auth.verify_token(token)
    assert (await auth.verify_token(newer))["sub"] == sub
    other_user = make_token(iat=now - 60)
    assert await auth.verify_token(other_user)  # only that user is affected


async def test_redis_down_fails_closed_with_503(static_auth, monkeypatch):
    class DownRedis:
        async def mget(self, *_):
            raise RedisConnectionError("connection refused")

    monkeypatch.setattr(auth, "get_redis", lambda: DownRedis())
    with pytest.raises(ServiceUnavailableError):
        await auth.verify_token(make_token())


# ---------- JWKSClient ----------


async def test_jwks_is_cached():
    clock, identity = Clock(), FakeIdentity(("a", KEY_A))
    client = identity.client(clock)
    for _ in range(5):
        assert await client.get_key("a") is not None
    assert identity.calls == 1  # fetched once, then served from cache


async def test_jwks_refreshes_after_ttl():
    clock, identity = Clock(), FakeIdentity(("a", KEY_A))
    client = identity.client(clock)
    await client.get_key("a")
    clock.t += 601  # past the 10-minute cache
    await client.get_key("a")
    assert identity.calls == 2


async def test_unknown_kid_triggers_one_refresh_for_key_rotation():
    clock, identity = Clock(), FakeIdentity(("a", KEY_A))
    client = identity.client(clock)
    await client.get_key("a")
    identity.body = jwks_body(("a", KEY_A), ("b", KEY_B))  # Identity rotates: publishes key b
    clock.t += 31
    assert await client.get_key("b") is not None  # found right away, not after 10 minutes
    assert identity.calls == 2


async def test_unknown_kid_refresh_is_throttled():
    clock, identity = Clock(), FakeIdentity(("a", KEY_A))
    client = identity.client(clock)
    await client.get_key("a")
    for i in range(20):  # attacker sends 20 tokens with made-up kids
        assert await client.get_key(f"garbage-{i}") is None
    assert identity.calls == 1  # within 30 s: no extra fetches
    clock.t += 31
    await client.get_key("garbage-x")
    assert identity.calls == 2  # one more allowed after the interval


async def test_failed_refresh_keeps_old_keys():
    clock, identity = Clock(), FakeIdentity(("a", KEY_A))
    client = identity.client(clock)
    await client.get_key("a")
    identity.fail = True
    clock.t += 601
    assert await client.get_key("a") is not None  # Identity down: cached key still works


async def test_no_keys_at_all_is_503():
    clock, identity = Clock(), FakeIdentity(("a", KEY_A))
    identity.fail = True
    with pytest.raises(ServiceUnavailableError):
        await identity.client(clock).get_key("a")


# ---------- FastAPI dependencies: 401 vs 403 ----------


@pytest.fixture
def app_client(static_auth):
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/me")
    async def me(user: CurrentUser = Depends(get_current_user)):
        return {"user_id": str(user.user_id), "role": user.role}

    @app.post("/jobs")
    async def create_job(user: CurrentUser = Depends(require_role("RECRUITER"))):
        return {"ok": True}

    @app.get("/board")
    async def board(user: CurrentUser | None = Depends(auth.get_optional_user)):
        return {"user": user.role if user else None}

    return TestClient(app)


def test_dependencies_401_vs_403(app_client):
    bearer = lambda t: {"Authorization": f"Bearer {t}"}  # noqa: E731
    assert app_client.get("/me").status_code == 401  # no token
    assert app_client.get("/me", headers=bearer("garbage")).status_code == 401  # bad token
    assert app_client.get("/me", headers=bearer(make_token())).json()["role"] == "CANDIDATE"
    r = app_client.post("/jobs", headers=bearer(make_token(role="CANDIDATE")))
    assert r.status_code == 403  # valid token, wrong role
    assert app_client.post("/jobs", headers=bearer(make_token(role="RECRUITER"))).status_code == 200


def test_ensure_owner():
    me = CurrentUser(user_id=uuid.uuid4(), role="RECRUITER", jti="j", claims={})
    ensure_owner(me.user_id, me)  # own resource: fine
    with pytest.raises(ForbiddenError):
        ensure_owner(uuid.uuid4(), me)  # someone else's: 403


def test_optional_user(app_client):
    assert app_client.get("/board").json() == {"user": None}  # anonymous is fine
    ok = app_client.get("/board", headers={"Authorization": f"Bearer {make_token(role='RECRUITER')}"})
    assert ok.json() == {"user": "RECRUITER"}
    # A bad token is NOT treated as anonymous
    assert app_client.get("/board", headers={"Authorization": "Bearer garbage"}).status_code == 401


def test_token_factory_tokens_verify():
    from smarthire_common.testing import TokenFactory

    tokens = TokenFactory()
    auth.init_auth(tokens.key_provider, issuer=tokens.issuer, audience=tokens.audience)
    try:
        import asyncio

        import fakeredis

        fake = fakeredis.FakeAsyncRedis(decode_responses=True)
        original = auth.get_redis
        auth.get_redis = lambda: fake
        claims = asyncio.run(auth.verify_token(tokens.mint(role="RECRUITER")))
        assert claims["role"] == "RECRUITER"
    finally:
        auth.get_redis = original
        auth._config = None
