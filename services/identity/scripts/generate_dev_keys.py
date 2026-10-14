"""Generate the RSA key pair Identity signs JWTs with (local development only).

    cd services/identity && uv run python scripts/generate_dev_keys.py      (or: make keys)

Writes <repo>/secrets/identity/jwt_private.pem (+ jwt_public.pem for inspection, e.g. jwt.io).
secrets/ is gitignored. In production the key comes from a secrets manager instead."""

import argparse
import sys
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

OUT_DIR = Path(__file__).resolve().parents[3] / "secrets" / "identity"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="overwrite an existing key")
    args = parser.parse_args()

    private_path, public_path = OUT_DIR / "jwt_private.pem", OUT_DIR / "jwt_public.pem"
    if private_path.exists() and not args.force:
        # Replacing the key invalidates every token already issued: make that a deliberate act
        sys.exit(f"{private_path} already exists (use --force to replace it and log everyone out)")

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    private_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    public_path.write_bytes(
        key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )
    # Readable by the container's non-root user through the bind mount. Fine for a local dev
    # key; never do this with a real key.
    private_path.chmod(0o644)
    print(f"Wrote {private_path}\nWrote {public_path}")


if __name__ == "__main__":
    main()
