from pathlib import Path

from smarthire_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "identity"
    port: int = 8001
    database_url: str  # postgresql+asyncpg://identity_user:<pw>@postgres:5432/smarthire
    db_schema: str = "identity"
    redis_url: str = "redis://localhost:6379/0"

    # --- JWT (access tokens) ---
    # PEM file of the RSA private key. Generate with scripts/generate_dev_keys.py. Never commit it.
    jwt_private_key_path: Path
    jwt_issuer: str = "smarthire-identity"  # "iss": who issued the token
    jwt_audience: str = "smarthire"  # "aud": who the token is meant for
    # Where verifiers (the gateway) fetch our public keys; published in the discovery document.
    # Internal Compose address: only the gateway and services read it.
    jwks_public_url: str = "http://identity:8001/.well-known/jwks.json"
    access_token_ttl_seconds: int = 15 * 60

    # --- Refresh tokens (httpOnly cookie) ---
    refresh_token_ttl_days: int = 14
    refresh_cookie_name: str = "refresh_token"
    # Browser only sends the cookie to paths under this prefix. Behind the gateway (Phase 4)
    # the public path becomes /api/identity/auth, so this must be updated then.
    refresh_cookie_path: str = "/auth"
    refresh_cookie_secure: bool = True  # only sent over HTTPS (browsers treat localhost as secure)

    # --- Login rate limiting (failed attempts per window) ---
    login_max_failures_per_email: int = 5
    login_max_failures_per_ip: int = 20
    login_failure_window_seconds: int = 15 * 60
