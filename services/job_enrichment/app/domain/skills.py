"""Keyword extraction for job descriptions (pure, no I/O). The skills dictionary itself is shared:
smarthire_common.skills (Candidate and Scoring use the same one)."""

import yake
from smarthire_common.skills import SKILLS, extract_skills, normalize  # noqa: F401  (re-exported)


def extract_keywords(text: str, top: int = 10) -> list[str]:
    """Phrases that stand out in this text (YAKE: statistics of the text itself, no model, no network).
    Lowercased and de-duplicated, best first."""
    if not text.strip():
        return []
    extractor = yake.KeywordExtractor(lan="en", n=2, top=top, dedupLim=0.7)
    keywords = [kw.lower() for kw, _score in extractor.extract_keywords(text)]
    return list(dict.fromkeys(keywords))
