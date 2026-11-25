"""Step 10.2: resume parsing. Pure rules first, then the use case on real Postgres + MinIO (called directly;
in production it is ResumeProcessingWorkflow's parse_resume step, served by candidate-parser)."""

import uuid
from datetime import date

import pytest
from smarthire_common import db, testing
from sqlalchemy import text

from app.domain.resume_parser import ResumeUnreadableError, education, experience_years, extract_text, parse
from app.models import Candidate, Resume
from app.models.candidate import DOCX, PDF
from app.repositories import files
from app.services import parsing
from tests.conftest import BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, TABLES
from tests.resume_files import RESUME_LINES, make_docx, make_pdf

TODAY = date(2026, 11, 3)

# ---------------------------------------------------------------- pure parsing


@pytest.mark.parametrize(("data", "content_type"), [(make_pdf(RESUME_LINES), PDF), (make_docx(RESUME_LINES), DOCX)])
def test_text_comes_out_of_pdf_and_word_files(data, content_type):
    assert "Acme Corp" in extract_text(data, content_type)


def test_parse_finds_skills_experience_and_education():
    parsed = parse("\n".join(RESUME_LINES), TODAY)
    assert parsed["skills"] == ["docker", "fastapi", "kubernetes", "postgresql", "python"]  # shared dictionary
    assert parsed["experience_years"] == 9.0  # 2017-2019 + 2019-2026, the overlap counted once
    assert parsed["education"] == ["Bachelor"]


@pytest.mark.parametrize(("text_", "years"), [
    ("5+ years of Python", 5.0),
    ("3 yrs Java; 2018 - 2020", 3.0),  # the claim beats a shorter range
    ("2015 to 2016, 2020-2022", 3.0),
    ("no dates here", None),
    ("2030 - 2031", None),  # in the future: ignored
])
def test_experience_years(text_, years):
    assert experience_years(text_, TODAY) == years


def test_education_levels():
    assert education("PhD in Physics, MSc Math") == ["PhD", "Master"]


@pytest.mark.parametrize("data", [b"%PDF-1.4 broken", make_pdf([])])
def test_unreadable_or_empty_files(data):
    with pytest.raises(ResumeUnreadableError):
        extract_text(data, PDF)


# ---------------------------------------------------------------- the use case (Postgres + MinIO)


@pytest.fixture
async def stores(migrated, database_url, minio):
    await testing.truncate(database_url, TABLES)
    endpoint, s3 = minio
    for obj in list(s3.list_objects(BUCKET, recursive=True)):
        s3.remove_object(BUCKET, obj.object_name)
    db.init_db(database_url)  # no app runs in these tests
    files.init_storage(endpoint, S3_ACCESS_KEY, S3_SECRET_KEY, bucket=BUCKET, secure=False)
    yield
    await db.close_db()


async def uploaded(data: bytes, content_type: str = PDF) -> uuid.UUID:
    candidate_id, resume_id = uuid.uuid4(), uuid.uuid4()
    key = f"{candidate_id}/{resume_id}"
    await files.put(key, data, content_type)
    async with db.session_scope() as s:
        s.add(Candidate(id=candidate_id))
        await s.flush()
        s.add(Resume(id=resume_id, candidate_id=candidate_id, object_key=key, file_name="cv", content_type=content_type,
                     size_bytes=len(data)))
    return resume_id


async def one(sql: str):
    async with db.session_scope() as s:
        return (await s.execute(text(sql))).one()


async def test_parse_saves_the_parsed_data(stores):
    resume_id = await uploaded(make_pdf(RESUME_LINES))
    assert await parsing.parse_resume(resume_id) == "parsed"
    status, parsed = await one("SELECT status, parsed_data FROM resumes")
    assert status == "PARSED" and "python" in parsed["skills"] and parsed["education"] == ["Bachelor"]


async def test_running_twice_parses_once(stores):
    """Temporal may run the parse step again after a timeout: the second run must do nothing."""
    resume_id = await uploaded(make_docx(RESUME_LINES), DOCX)
    assert await parsing.parse_resume(resume_id) == "parsed"
    assert await parsing.parse_resume(resume_id) == "skipped"


async def test_unreadable_file_is_failed_not_retried(stores):
    resume_id = await uploaded(b"%PDF-1.4 this is not really a pdf")
    assert await parsing.parse_resume(resume_id) == "failed"  # returns normally: the endpoint answers 422
    status, error = await one("SELECT status, parse_error FROM resumes")
    assert status == "FAILED" and "could not read" in error


async def test_unknown_resume_is_missing(stores):
    assert await parsing.parse_resume(uuid.uuid4()) == "missing"
