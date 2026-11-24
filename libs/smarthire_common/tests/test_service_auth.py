"""Design v2.1: orchestrator tokens on internal endpoints, and the Idempotency-Key contract."""

import time

import httpx
import pytest
from fastapi import Depends, FastAPI

from smarthire_common.errors import register_exception_handlers
from smarthire_common.internal import idempotency_key
from smarthire_common.service_auth import (
    AUDIENCE,
    ISSUER,
    ServiceTokenSigner,
    close_service_auth,
    init_service_auth,
    require_orchestrator,
)
from smarthire_common.testing import ServiceTokenFactory, TokenFactory


@pytest.fixture
async def client():
    tokens = ServiceTokenFactory()
    init_service_auth(tokens.key_provider)
    app = FastAPI()
    register_exception_handlers(app)

    @app.post("/internal/things", dependencies=[Depends(require_orchestrator)])
    async def create(key: str = Depends(idempotency_key)):
        return {"idempotency_key": key}

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as c:
        c.tokens = tokens
        yield c
    await close_service_auth()


async def test_orchestrator_token_and_key_pass(client):
    r = await client.post("/internal/things", headers=client.tokens.headers(idempotency_key="wf-1:3"))
    assert r.status_code == 200
    assert r.json() == {"idempotency_key": "wf-1:3"}


async def test_missing_token_is_401(client):
    r = await client.post("/internal/things", headers={"Idempotency-Key": "k"})
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "UNAUTHORIZED"


async def test_missing_idempotency_key_is_422(client):
    r = await client.post("/internal/things", headers=client.tokens.signer.headers())
    assert r.status_code == 422


async def test_user_token_is_not_a_service_token(client):
    # Signed by some key the service doesn't trust, AND the wrong issuer/audience
    r = await client.post("/internal/things", headers={**TokenFactory().headers(role="ADMIN"), "Idempotency-Key": "k"})
    assert r.status_code == 401


@pytest.mark.parametrize("claims", [{"iss": "smarthire-identity"}, {"aud": "smarthire"}])
async def test_wrong_issuer_or_audience_is_401(client, claims):
    import jwt

    signer = client.tokens.signer
    now = int(time.time())
    payload = {"sub": "orchestrator", "iss": ISSUER, "aud": AUDIENCE, "iat": now, "exp": now + 60, **claims}
    token = jwt.encode(payload, signer._key, algorithm="RS256", headers={"kid": signer.kid})
    r = await client.post("/internal/things", headers={"Authorization": f"Bearer {token}", "Idempotency-Key": "k"})
    assert r.status_code == 401


async def test_expired_token_is_401(client):
    token = client.tokens.signer.mint(now=int(time.time()) - 600)  # expired 9 minutes ago
    r = await client.post("/internal/things", headers={"Authorization": f"Bearer {token}", "Idempotency-Key": "k"})
    assert r.status_code == 401


async def test_token_from_another_orchestrator_key_is_401(client):
    other = ServiceTokenFactory()  # right issuer and audience, untrusted key
    r = await client.post("/internal/things", headers=other.headers())
    assert r.status_code == 401


def test_jwks_publishes_the_signing_key():
    from cryptography.hazmat.primitives.asymmetric import rsa

    signer = ServiceTokenSigner(rsa.generate_private_key(public_exponent=65537, key_size=2048))
    (jwk,) = signer.jwks()["keys"]
    assert jwk["kid"] == signer.kid and jwk["alg"] == "RS256" and jwk["kty"] == "RSA" and "d" not in jwk
