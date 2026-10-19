from fastapi import APIRouter, Depends, Response

from app.api.deps import get_settings, get_signing_key
from app.config import Settings
from app.domain.tokens import SigningKey, public_jwk

router = APIRouter(tags=["jwks"])


@router.get("/.well-known/jwks.json")
async def jwks(response: Response, key: SigningKey = Depends(get_signing_key)) -> dict:
    """Public keys only: safe to publish. A list, so during key rotation old and new keys can be
    published side by side and verifiers pick by kid."""
    response.headers["Cache-Control"] = "public, max-age=600"  # verifiers may cache 10 minutes
    return {"keys": [public_jwk(key)]}


@router.get("/.well-known/openid-configuration")
async def openid_configuration(response: Response, settings: Settings = Depends(get_settings)) -> dict:
    """Minimal OIDC discovery document. Gateways (APISIX openid-connect) read it to find our
    issuer and where our public keys live. We are not a full OpenID provider: we only support
    verifying our own RS256 access tokens, so login/token endpoints are not advertised here."""
    response.headers["Cache-Control"] = "public, max-age=600"
    return {
        "issuer": settings.jwt_issuer,
        "jwks_uri": settings.jwks_public_url,
        "id_token_signing_alg_values_supported": ["RS256"],
        "subject_types_supported": ["public"],
        "response_types_supported": ["token"],
    }
