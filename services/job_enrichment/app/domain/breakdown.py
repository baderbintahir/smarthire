"""Split a job description into requirements, responsibilities and benefits (pure, no I/O).

Two passes:
1. Headings: lines like "Requirements:", "What you'll do", "Perks" start a section; the lines under
   them belong to it.
2. Keywords: sentences outside any heading are sorted by tell-tale words ("experience with" ->
   requirement, "you will" -> responsibility, "we offer" -> benefit). The rest is left out."""

import re

SECTIONS = ("requirements", "responsibilities", "benefits")

HEADINGS: dict[str, tuple[str, ...]] = {
    "requirements": ("requirements", "qualifications", "what you bring", "what we're looking for",
                     "what we are looking for", "who you are", "must have", "must-have", "nice to have",
                     "skills", "about you"),
    "responsibilities": ("responsibilities", "what you'll do", "what you will do", "your role", "the role",
                         "duties", "day to day", "day-to-day", "your impact"),
    "benefits": ("benefits", "perks", "what we offer", "why join us", "why us", "compensation"),
}

KEYWORDS: dict[str, tuple[str, ...]] = {
    "requirements": ("experience with", "experience in", "years of", "proficien", "knowledge of", "familiar",
                     "degree", "must have", "required", "strong understanding", "you have"),
    # Phrases, not bare verbs: "build" alone would also catch intros like "we're hiring to build ..."
    "responsibilities": ("you will", "you'll", "responsible for", "your role", "you'd", "collaborate with",
                         "day-to-day you"),
    "benefits": ("we offer", "benefit", "insurance", "vacation", "paid time off", "pto", "remote", "salary",
                 "bonus", "equity", "learning budget", "flexible"),
}

_BULLET = re.compile(r"^\s*(?:[-*•·]|\d+[.)])\s*")


def _heading_section(line: str) -> str | None:
    text = line.strip().rstrip(":").strip().lower()
    if not text or len(text) > 40:  # headings are short
        return None
    for section, names in HEADINGS.items():
        if any(text == name or text.startswith(name) for name in names):
            return section
    return None


def _keyword_section(sentence: str) -> str | None:
    lowered = sentence.lower()
    for section in SECTIONS:  # fixed order: the same sentence always lands in the same section
        if any(word in lowered for word in KEYWORDS[section]):
            return section
    return None


def breakdown(description: str) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {section: [] for section in SECTIONS}
    current: str | None = None
    for raw_line in description.splitlines():
        if (section := _heading_section(raw_line)) is not None:
            current = section
            continue
        line = _BULLET.sub("", raw_line).strip()
        if not line:
            continue
        if current is not None:
            result[current].append(line)
            continue
        for sentence in re.split(r"(?<=[.!?])\s+", line):  # free text: judge sentence by sentence
            if (section := _keyword_section(sentence)) is not None:
                result[section].append(sentence.strip())
    return result
