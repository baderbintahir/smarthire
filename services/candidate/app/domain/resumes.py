"""Resume file rules (pure functions, no I/O).

The file's TYPE is decided from its first bytes ("magic numbers"), never from what the client
says: the Content-Type header and the file name are both just text the uploader chose."""

import io
import re
import uuid
import zipfile
from pathlib import PurePosixPath, PureWindowsPath

from app.domain.errors import FileTooLargeError, UnsupportedFileTypeError
from app.models.candidate import DOCX, PDF


def check_size(size: int, limit: int) -> None:
    if size > limit:
        raise FileTooLargeError(limit)
    if size == 0:
        raise UnsupportedFileTypeError()  # an empty file is not a resume


def detect_content_type(data: bytes) -> str:
    """PDF or DOCX, judged by content. Anything else (images, an .exe renamed to .pdf, old .doc,
    a random zip) raises UnsupportedFileTypeError."""
    if data.startswith(b"%PDF-"):
        return PDF
    # A .docx is a zip archive with a Word document inside it
    if data.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                names = set(archive.namelist())  # the zip's table of contents; nothing is extracted
        except zipfile.BadZipFile:
            raise UnsupportedFileTypeError() from None
        if {"[Content_Types].xml", "word/document.xml"} <= names:
            return DOCX
    raise UnsupportedFileTypeError()


def safe_file_name(name: str | None) -> str:
    """The uploader's file name, kept for display only (the object key never uses it). Drops any
    folder part (both / and \\ styles) and control characters, and caps the length."""
    base = PureWindowsPath(PurePosixPath(name or "").name).name
    base = re.sub(r"[\x00-\x1f\x7f]", "", base).strip()
    return base[:255] or "resume"


def object_key(candidate_id: uuid.UUID, resume_id: uuid.UUID) -> str:
    """Where the file lives in the `resumes` bucket. Built from ids only: unguessable, and nothing
    the user typed ends up in a storage path."""
    return f"{candidate_id}/{resume_id}"
