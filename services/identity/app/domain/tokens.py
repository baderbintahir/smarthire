"""Access tokens (RS256 JWT) and refresh tokens (random opaque strings).

RS256 = RSA signature + SHA-256. Identity signs with the PRIVATE key; every service (Identity included) verifies with the
PUBLIC key via smarthire_common.auth. Only Identity can mint tokens.
The JWT payload is base64, NOT encrypted: anyone can read it, so it holds no secrets."""

import base64
import hashlib
import json
import secrets
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
from jwt.algorithms import RSAAlgorithm

ALGORITHM = "RS256"


@dataclass(frozen=True)
class SigningKey:
    private_key: RSAPrivateKey
    public_key: RSAPublicKey
    kid: str  # key id: put in the JWT header so verifiers know which public key to use


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def compute_kid(public_key: RSAPublicKey) -> str:
    """RFC 7638 JWK thumbprint: a hash of the public key. The same key always gets the same kid,
    and a new key automatically gets a new one (no config to keep in sync)."""
    numbers = public_key.public_numbers()
    jwk = {
        "e": _b64url(numbers.e.to_bytes((numbers.e.bit_length() + 7) // 8, "big")),
        "kty": "RSA",
        "n": _b64url(numbers.n.to_bytes((numbers.n.bit_length() + 7) // 8, "big")),
    }
    canonical = json.dumps(jwk, separators=(",", ":"), sort_keys=True).encode()
    return _b64url(hashlib.sha256(canonical).digest())


def load_signing_key(pem_path: Path) -> SigningKey:
    private_key = serialization.load_pem_private_key(pem_path.read_bytes(), password=None)
    if not isinstance(private_key, RSAPrivateKey):
        raise ValueError(f"{pem_path} is not an RSA private key")
    public_key = private_key.public_key()
    return SigningKey(private_key, public_key, compute_kid(public_key))


def create_access_token(
    key: SigningKey,
    *,
    user_id: uuid.UUID,
    role: str,
    token_version: int,
    issuer: str,
    audience: str,
    ttl_seconds: int,
    now: datetime | None = None,
) -> tuple[str, dict]:
    now = now or datetime.now(UTC)
    claims = {
        "sub": str(user_id),  # subject: who the token is about
        "role": role,  # lets services authorise without calling Identity
        "tv": token_version,  # token_version: "logout everywhere" invalidates older tokens
        "iss": issuer,  # issuer: who created it
        "aud": audience,  # audience: who should accept it
        "iat": int(now.timestamp()),  # issued at
        "exp": int((now + timedelta(seconds=ttl_seconds)).timestamp()),  # expiry: short-lived
        "jti": str(uuid.uuid4()),  # unique token id: lets an admin revoke this exact token
    }
    token = jwt.encode(claims, key.private_key, algorithm=ALGORITHM, headers={"kid": key.kid})
    return token, claims


def public_jwk(key: SigningKey) -> dict:
    """The public key in JWK format, as published at /.well-known/jwks.json."""
    jwk = RSAAlgorithm.to_jwk(key.public_key, as_dict=True)
    return {**jwk, "kid": key.kid, "alg": ALGORITHM, "use": "sig"}


def new_refresh_token() -> str:
    """256 bits of randomness. Opaque: it means nothing by itself, only its DB row does."""
    return secrets.token_urlsafe(32)


def hash_refresh_token(token: str) -> bytes:
    """SHA-256 is enough here (unlike passwords): the token is already long and random, so it
    cannot be guessed, only stolen. Hashing means a DB leak doesn't hand out live tokens."""
    return hashlib.sha256(token.encode()).digest()
