"""Profile rules (pure functions, no I/O)."""


def normalize_skills(skills: list[str]) -> list[str]:
    """Lowercase, trimmed, no duplicates, order kept. The same rule as Job's required_skills, so
    Scoring (Phase 10) can compare a candidate's skills with a job's directly."""
    seen: dict[str, None] = {}  # a dict keeps insertion order: an "ordered set"
    for skill in skills:
        seen.setdefault(skill.strip().lower(), None)
    return [s for s in seen if s]
