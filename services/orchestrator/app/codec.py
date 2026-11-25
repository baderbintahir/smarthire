"""Payload encryption (design v2.0 "Encrypted workflow payloads"; proven in the Step T1 spike).

Temporal stores every workflow input, step argument and result in its history (its database, its UI).
This codec runs in the orchestrator only, on the way out and on the way back, so Temporal itself only
ever sees ciphertext: emails, names and resume facts stay unreadable there.

NOT imported by workflow modules: workflow code runs in a sandbox that re-imports its modules, and
cryptography's compiled extension can't be loaded twice (found in the spike)."""

import dataclasses
import os
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from temporalio.api.common.v1 import Payload
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.converter import default as default_converter

ENCODING = b"binary/encrypted"
NONCE_BYTES = 12  # AES-GCM: a fresh random nonce per payload


class EncryptionCodec(PayloadCodec):
    def __init__(self, key: bytes) -> None:
        if len(key) != 32:
            raise ValueError("the payload key must be 32 bytes (AES-256)")
        self._aes = AESGCM(key)

    async def encode(self, payloads):
        out = []
        for p in payloads:
            nonce = os.urandom(NONCE_BYTES)
            # The whole payload (its metadata too) is encrypted, so even the original encoding is hidden
            out.append(Payload(metadata={"encoding": ENCODING},
                               data=nonce + self._aes.encrypt(nonce, p.SerializeToString(), None)))
        return out

    async def decode(self, payloads):
        out = []
        for p in payloads:
            if p.metadata.get("encoding") != ENCODING:
                out.append(p)  # not encrypted (e.g. written by a tool): pass through
                continue
            # A wrong key raises InvalidTag: the history can't be read, never silently misread
            out.append(Payload.FromString(self._aes.decrypt(p.data[:NONCE_BYTES], p.data[NONCE_BYTES:], None)))
        return out


def load_payload_key(path: str | Path) -> bytes:
    return bytes.fromhex(Path(path).read_text().strip())


def encrypted_converter(key: bytes) -> DataConverter:
    return dataclasses.replace(default_converter(), payload_codec=EncryptionCodec(key))
