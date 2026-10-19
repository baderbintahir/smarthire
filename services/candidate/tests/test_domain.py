"""Pure rules: no database, no network."""

import io
import uuid
import zipfile

import pytest

from app.domain import resumes
from app.domain.errors import FileTooLargeError, UnsupportedFileTypeError
from app.domain.profile import normalize_skills
from app.models.candidate import DOCX, PDF


def make_zip(*names: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name in names:
            archive.writestr(name, "<xml/>")
    return buffer.getvalue()


MINIMAL_PDF = b"%PDF-1.4\n1 0 obj <<>> endobj\ntrailer <<>>\n%%EOF\n"
MINIMAL_DOCX = make_zip("[Content_Types].xml", "word/document.xml")


# ---------- file type: decided by content ----------


def test_pdf_is_detected_by_its_magic_bytes():
    assert resumes.detect_content_type(MINIMAL_PDF) == PDF


def test_docx_is_a_zip_with_a_word_document_inside():
    assert resumes.detect_content_type(MINIMAL_DOCX) == DOCX


@pytest.mark.parametrize(
    "data",
    [
        b"\x89PNG\r\n\x1a\n....",  # an image
        b"MZ\x90\x00 executable",  # a Windows .exe (renaming it cv.pdf doesn't change its bytes)
        b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",  # old binary .doc (not supported)
        b"PK\x03\x04 not really a zip",  # zip signature, broken archive
        make_zip("hello.txt"),  # a real zip, but not a Word document
        make_zip("[Content_Types].xml", "xl/workbook.xml"),  # an Excel .xlsx
        b"plain text pretending %PDF-",  # the marker must be at the very start
    ],
)
def test_everything_else_is_rejected(data):
    with pytest.raises(UnsupportedFileTypeError):
        resumes.detect_content_type(data)


# ---------- size ----------


def test_size_limit_is_inclusive():
    resumes.check_size(100, limit=100)
    with pytest.raises(FileTooLargeError):
        resumes.check_size(101, limit=100)


def test_empty_file_is_rejected():
    with pytest.raises(UnsupportedFileTypeError):
        resumes.check_size(0, limit=100)


# ---------- names and keys ----------


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("cv.pdf", "cv.pdf"),
        ("../../etc/passwd", "passwd"),  # no folders, no path tricks
        ("C:\\Users\\rita\\My CV.docx", "My CV.docx"),
        ("evil\r\nname.pdf", "evilname.pdf"),  # no control characters (log/header injection)
        ("", "resume"),
        (None, "resume"),
        ("x" * 300 + ".pdf", "x" * 255),
    ],
)
def test_safe_file_name(given, expected):
    assert resumes.safe_file_name(given) == expected


def test_object_key_is_made_of_ids_only():
    candidate_id, resume_id = uuid.uuid4(), uuid.uuid4()
    assert resumes.object_key(candidate_id, resume_id) == f"{candidate_id}/{resume_id}"


# ---------- skills ----------


def test_skills_are_lowercased_trimmed_and_unique_in_order():
    assert normalize_skills(["Python", " python ", "SQL", "", "FastAPI"]) == ["python", "sql", "fastapi"]
