"""Generate the orchestrator's two keys (local development only; design v2.1).

    cd services/orchestrator && uv run python scripts/generate_dev_keys.py      (or: make keys)

Writes into <repo>/secrets/orchestrator/ (gitignored):
    service_private.pem  RSA key that signs service tokens (internal endpoints verify them via its JWKS)
    payload.key          AES-256 key (64 hex characters) that encrypts every workflow payload in Temporal

Existing keys are kept: a new payload key makes every stored workflow history unreadable, and a new
signing key is only picked up by services after their JWKS cache refreshes. Pass --force to replace."""

import argparse
import os
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

OUT_DIR = Path(__file__).resolve().parents[3] / "secrets" / "orchestrator"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="replace existing keys")
    args = parser.parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    signing = OUT_DIR / "service_private.pem"
    if signing.exists() and not args.force:
        print(f"{signing} exists, kept")
    else:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        signing.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                              serialization.NoEncryption()))
        signing.chmod(0o644)  # readable by the container's non-root user; fine for a dev key only
        print(f"Wrote {signing}")

    payload = OUT_DIR / "payload.key"
    if payload.exists() and not args.force:
        print(f"{payload} exists, kept")
    else:
        payload.write_text(os.urandom(32).hex() + "\n")
        payload.chmod(0o644)
        print(f"Wrote {payload}")


if __name__ == "__main__":
    main()
