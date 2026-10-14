"""Unit tests: pure domain code, no database, no HTTP."""

import uuid
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from app.domain import passwords, tokens


def test_hash_is_salted_and_verifies():
    h1, h2 = passwords.hash_password("s3cret-pass"), passwords.hash_password("s3cret-pass")
    assert h1 != h2  # different random salt each time
    assert h1.startswith("$argon2id$")
    assert passwords.verify_password(h1, "s3cret-pass")
    assert not passwords.verify_password(h1, "wrong-pass")
    assert not passwords.verify_password("not-a-hash", "s3cret-pass")  # garbage never raises


@pytest.fixture(scope="module")
def key() -> tokens.SigningKey:
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return tokens.SigningKey(private, private.public_key(), tokens.compute_kid(private.public_key()))


def _token(key, **overrides):
    args = dict(
        user_id=uuid.uuid4(), role="CANDIDATE", token_version=1,
        issuer="smarthire-identity", audience="smarthire", ttl_seconds=900,
    )
    return tokens.create_access_token(key, **{**args, **overrides})


def _decode(key, token, **kw):
    # Same checks as smarthire_common.auth.verify_token (the real verifier, tested in the chassis)
    return jwt.decode(
        token, key.public_key, algorithms=["RS256"],
        issuer=kw.get("issuer", "smarthire-identity"), audience=kw.get("audience", "smarthire"),
    )


def test_access_token_claims_and_header(key):
    token, claims = _token(key)
    assert jwt.get_unverified_header(token) == {"alg": "RS256", "kid": key.kid, "typ": "JWT"}
    decoded = _decode(key, token)
    assert decoded == claims
    assert decoded["exp"] - decoded["iat"] == 900
    assert set(decoded) == {"sub", "role", "tv", "iss", "aud", "iat", "exp", "jti"}


def test_expired_token_rejected(key):
    token, _ = _token(key, now=datetime.now(UTC) - timedelta(hours=1))
    with pytest.raises(jwt.ExpiredSignatureError):
        _decode(key, token)


def test_wrong_audience_or_issuer_rejected(key):
    token, _ = _token(key)
    with pytest.raises(jwt.InvalidAudienceError):
        _decode(key, token, audience="someone-else")
    with pytest.raises(jwt.InvalidIssuerError):
        _decode(key, token, issuer="evil-issuer")


def test_token_signed_by_another_key_rejected(key):
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    forged = jwt.encode({"sub": "x"}, other, algorithm="RS256")
    with pytest.raises(jwt.InvalidSignatureError):
        _decode(key, forged)


def test_tampered_payload_rejected(key):
    token, _ = _token(key)
    header, payload, signature = token.split(".")
    # Flip one character of the payload: the signature no longer matches
    tampered = f"{header}.{payload[:-2]}{'A' if payload[-2] != 'A' else 'B'}{payload[-1]}.{signature}"
    with pytest.raises(jwt.InvalidTokenError):
        _decode(key, tampered)


def test_alg_none_rejected(key):
    unsigned = jwt.encode({"sub": "x", "iss": "smarthire-identity", "aud": "smarthire"}, None, algorithm="none")
    with pytest.raises(jwt.InvalidTokenError):
        _decode(key, unsigned)


def test_kid_is_stable_thumbprint(key):
    assert tokens.compute_kid(key.public_key) == key.kid
    assert tokens.public_jwk(key)["kid"] == key.kid


def test_refresh_tokens_random_and_hashed():
    t1, t2 = tokens.new_refresh_token(), tokens.new_refresh_token()
    assert t1 != t2 and len(t1) >= 43
    assert tokens.hash_refresh_token(t1) == tokens.hash_refresh_token(t1)
    assert len(tokens.hash_refresh_token(t1)) == 32  # SHA-256 digest bytes
