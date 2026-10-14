from fastapi import APIRouter, Depends, Response

from app.api.deps import get_signing_key
from app.domain.tokens import SigningKey, public_jwk

router = APIRouter(tags=["jwks"])


@router.get("/.well-known/jwks.json")
async def jwks(response: Response, key: SigningKey = Depends(get_signing_key)) -> dict:
    """Public keys only: safe to publish. A list, so during key rotation old and new keys can be
    published side by side and verifiers pick by kid."""
    response.headers["Cache-Control"] = "public, max-age=600"  # verifiers may cache 10 minutes
    return {"keys": [public_jwk(key)]}
