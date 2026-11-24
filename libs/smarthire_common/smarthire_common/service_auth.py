"""Service tokens (design v2.1): how an internal endpoint knows the caller is the orchestrator.

The orchestrator signs a short-lived RS256 token for every workflow step it runs; internal endpoints
that change data require it. Same idea as user tokens (Identity signs, everyone verifies with its
JWKS), with a different issuer and audience, so a user token can never pass as a service token and
the other way round.

    orchestrator:  signer = ServiceTokenSigner.from_pem(path);  headers = signer.headers()
                   GET /.well-known/jwks.json -> signer.jwks()
    services:      startup: init_service_auth(JWKSClient(settings.orchestrator_jwks_url))
                   routes:  Depends(require_orchestrator)

No revocation check: a service token lives 60 seconds, and the only holder of the key is the
orchestrator (rotate the key to cut everything off)."""

import base64
import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt.algorithms import RSAAlgorithm

from smarthire_common.auth import ALGORITHM, JWKSClient, KeyProvider
from smarthire_common.errors import UnauthorizedError

ISSUER = "smarthire-orchestrator"
AUDIENCE = "smarthire-internal"
TTL_SECONDS = 60


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def compute_kid(public_key: RSAPublicKey) -> str:
    """RFC 7638 JWK thumbprint (same rule as Identity's keys): a new key gets a new kid by itself."""
    numbers = public_key.public_numbers()
    jwk = {
        "e": _b64url(numbers.e.to_bytes((numbers.e.bit_length() + 7) // 8, "big")),
        "kty": "RSA",
        "n": _b64url(numbers.n.to_bytes((numbers.n.bit_length() + 7) // 8, "big")),
    }
    return _b64url(hashlib.sha256(json.dumps(jwk, separators=(",", ":"), sort_keys=True).encode()).digest())


class ServiceTokenSigner:
    """Only the orchestrator holds one. Mints a fresh token per call: cheap (one RSA signature) and
    never close to expiry, even when Temporal retries a step minutes later."""

    def __init__(self, private_key: RSAPrivateKey, *, subject: str = "orchestrator") -> None:
        self._key = private_key
        self.kid = compute_kid(private_key.public_key())
        self.subject = subject

    @classmethod
    def from_pem(cls, path: Path, **kwargs) -> "ServiceTokenSigner":
        key = serialization.load_pem_private_key(path.read_bytes(), password=None)
        if not isinstance(key, RSAPrivateKey):
            raise ValueError(f"{path} is not an RSA private key")
        return cls(key, **kwargs)

    def mint(self, *, ttl_seconds: int = TTL_SECONDS, now: int | None = None) -> str:
        now = int(time.time()) if now is None else now
        claims = {"sub": self.subject, "iss": ISSUER, "aud": AUDIENCE, "iat": now, "exp": now + ttl_seconds,
                  "jti": str(uuid.uuid4())}
        return jwt.encode(claims, self._key, algorithm=ALGORITHM, headers={"kid": self.kid})

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.mint()}"}

    def jwks(self) -> dict:
        jwk = RSAAlgorithm.to_jwk(self._key.public_key(), as_dict=True)
        return {"keys": [{**jwk, "kid": self.kid, "alg": ALGORITHM, "use": "sig"}]}


# ---------- verification, in every service with internal endpoints ----------


@dataclass
class _Config:
    key_provider: KeyProvider


_config: _Config | None = None


def init_service_auth(key_provider: KeyProvider) -> None:
    global _config
    _config = _Config(key_provider)


async def close_service_auth() -> None:
    global _config
    if _config is not None and isinstance(_config.key_provider, JWKSClient):
        await _config.key_provider.aclose()
    _config = None


async def verify_service_token(token: str) -> dict:
    """Claims of a valid orchestrator token; 401 for anything else, 503 if the keys can't be loaded."""
    if _config is None:
        raise RuntimeError("Service auth not initialised: call init_service_auth() at startup")
    invalid = UnauthorizedError("Invalid service token")
    try:
        kid = jwt.get_unverified_header(token).get("kid")
    except jwt.InvalidTokenError:
        raise invalid from None
    key = await _config.key_provider.get_key(kid) if kid else None
    if key is None:
        raise invalid
    try:
        return jwt.decode(token, key, algorithms=[ALGORITHM], issuer=ISSUER, audience=AUDIENCE,
                          options={"require": ["exp", "iat", "sub", "iss", "aud"]}, leeway=10)
    except jwt.InvalidTokenError:
        raise invalid from None


_bearer = HTTPBearer(auto_error=False)


async def require_orchestrator(credentials: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> dict:
    """`dependencies=[Depends(require_orchestrator)]` on every internal endpoint that changes data."""
    if credentials is None:
        raise UnauthorizedError("Missing service token")
    return await verify_service_token(credentials.credentials)
