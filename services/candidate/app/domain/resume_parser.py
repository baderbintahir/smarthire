"""Turn a resume file into structured data (pure functions, no I/O).

Rule-based, like the job side: the shared skills dictionary (smarthire_common.skills) plus a few
patterns for experience and education. Deterministic: the same file always gives the same result."""

import io
import re
from datetime import date

from smarthire_common.skills import extract_skills

from app.models.candidate import DOCX, PDF


class ResumeUnreadableError(Exception):
    """The file can't be read as text (corrupt, encrypted, scanned image...). Retrying won't help."""


def extract_text(data: bytes, content_type: str) -> str:
    try:
        if content_type == PDF:
            from pypdf import PdfReader

            text = "\n".join(page.extract_text() or "" for page in PdfReader(io.BytesIO(data)).pages)
        elif content_type == DOCX:
            from docx import Document

            document = Document(io.BytesIO(data))
            cells = [cell.text for table in document.tables for row in table.rows for cell in row.cells]
            text = "\n".join([p.text for p in document.paragraphs] + cells)
        else:
            raise ResumeUnreadableError(f"unsupported type {content_type}")
    except ResumeUnreadableError:
        raise
    except Exception as exc:  # every library error means the same thing here: we can't read it
        raise ResumeUnreadableError(f"could not read the file: {type(exc).__name__}") from exc
    if not text.strip():
        raise ResumeUnreadableError("no text found (a scanned image? OCR is not supported)")
    return text


_YEARS = re.compile(r"(\d{1,2}(?:\.\d)?)\s*\+?\s*(?:years?|yrs?)\b", re.IGNORECASE)
_RANGE = re.compile(r"\b((?:19|20)\d{2})\s*(?:-|–|—|to)\s*((?:19|20)\d{2}|present|current|now|today)\b", re.IGNORECASE)
_EDUCATION = {
    "PhD": re.compile(r"\b(ph\.?\s?d|doctorate)\b", re.IGNORECASE),
    "Master": re.compile(r"\b(master'?s?|m\.?sc|mba|m\.?s\.)\b", re.IGNORECASE),
    "Bachelor": re.compile(r"\b(bachelor'?s?|b\.?sc|b\.?s\.|b\.?a\.|b\.?e\.)\b", re.IGNORECASE),
}


def experience_years(text: str, today: date) -> float | None:
    """The larger of: an explicit "N years" claim, and the total of the date ranges (2019 - present),
    with overlapping ranges counted once. None when the text says nothing about it."""
    claimed = max((float(m.group(1)) for m in _YEARS.finditer(text)), default=0.0)
    spans = []
    for start, end in _RANGE.findall(text):
        last = today.year if not end[:1].isdigit() else int(end)
        if int(start) <= last <= today.year:
            spans.append((int(start), last))
    covered, cursor = 0, 0
    for start, end in sorted(spans):  # merge overlaps: 2018-2021 and 2020-2022 are 4 years, not 5
        start = max(start, cursor)
        if end > start:
            covered, cursor = covered + end - start, end
    best = min(max(claimed, float(covered)), 50.0)  # cap absurd values
    return round(best, 1) if best > 0 else None


def education(text: str) -> list[str]:
    return [level for level, pattern in _EDUCATION.items() if pattern.search(text)]


def parse(text: str, today: date) -> dict:
    return {
        "skills": extract_skills(text),
        "experience_years": experience_years(text, today),
        "education": education(text),
        "characters": len(text),
    }
