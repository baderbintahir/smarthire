"""Object storage (MinIO, S3-compatible) for resume files. A module-level client like the chassis'
db/redis helpers: init at startup, use anywhere.

The bucket is PRIVATE. Files are read only by our own code (the Phase 10 parser) or, later, via a
short-lived presigned link the service hands to someone it has authorized (Phase 12B). A public
bucket URL would let anyone holding the link read a person's CV, forever, with no access check."""

import asyncio
import io

from minio import Minio

_client: Minio | None = None
_bucket: str | None = None


def init_storage(endpoint: str, access_key: str, secret_key: str, *, bucket: str, secure: bool) -> None:
    global _client, _bucket
    _client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    _bucket = bucket


def _get() -> tuple[Minio, str]:
    if _client is None or _bucket is None:
        raise RuntimeError("Storage not initialised: call init_storage() at startup")
    return _client, _bucket


# The minio client is synchronous (blocking network calls). asyncio.to_thread runs each call in a
# worker thread, so one slow upload doesn't freeze every other request on the event loop.


async def put(key: str, data: bytes, content_type: str) -> None:
    client, bucket = _get()
    await asyncio.to_thread(client.put_object, bucket, key, io.BytesIO(data), len(data), content_type=content_type)


async def delete(key: str) -> None:
    client, bucket = _get()
    await asyncio.to_thread(client.remove_object, bucket, key)


async def storage_ping_check() -> None:
    """Readiness: the bucket is reachable with our credentials."""
    client, bucket = _get()
    if not await asyncio.to_thread(client.bucket_exists, bucket):
        raise RuntimeError(f"bucket {bucket!r} does not exist")
