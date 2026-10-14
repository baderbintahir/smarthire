"""Password hashing with Argon2id: slow and memory-hard on purpose, so a stolen hash is expensive
to brute-force. Each hash embeds its own random salt and parameters, e.g.
$argon2id$v=19$m=65536,t=3,p=4$<salt>$<hash>."""

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError

_hasher = PasswordHasher()  # library defaults follow the current OWASP recommendation


def hash_password(password: str) -> str:
    return _hasher.hash(password)  # new random salt every call


def verify_password(password_hash: str, password: str) -> bool:
    try:
        return _hasher.verify(password_hash, password)
    except (VerificationError, InvalidHashError):
        return False


def needs_rehash(password_hash: str) -> bool:
    """True if the hash was made with weaker parameters than today's defaults. On a successful
    login we re-hash, so stored hashes upgrade themselves over time."""
    return _hasher.check_needs_rehash(password_hash)


# Used when the email doesn't exist: we still run a full verify against this hash so a
# "no such user" answer takes as long as a "wrong password" answer (no timing leak).
DUMMY_HASH = hash_password("dummy-password-for-timing-equalisation")
