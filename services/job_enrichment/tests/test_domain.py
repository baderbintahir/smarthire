"""Pure extraction rules: no database, no Temporal."""

from app.domain.breakdown import breakdown
from app.domain.skills import extract_keywords, extract_skills, normalize

DESCRIPTION = """We are hiring a Backend Engineer to build Python APIs.

What you'll do:
- Design and run FastAPI services on Kubernetes
- Own our Kafka event pipeline

Requirements:
- 3+ years of experience with PostgreSQL
- Familiar with Docker and k8s

Benefits:
- Remote-first, flexible hours
- Learning budget
"""


def test_required_skills_come_first_normalized_then_found_ones_sorted():
    skills = extract_skills(DESCRIPTION, ["Python", "Postgres", "python"])
    assert skills[:2] == ["python", "postgresql"]  # required: normalized, de-duplicated, in order
    assert skills[2:] == sorted(skills[2:]) and {"fastapi", "kubernetes", "kafka", "docker"} <= set(skills)


def test_whole_words_only():
    assert extract_skills("A good engineer who goes to Gopher meetups", []) == []  # no "go"
    assert extract_skills("We use Go and C# and C++", []) == ["c#", "c++", "go"]


def test_aliases_map_to_one_name():
    assert normalize("K8s") == "kubernetes" and normalize("ReactJS") == "react" and normalize("Haskell") == "haskell"


def test_same_input_same_output():
    """Retries must give the same answer, so the extraction is deterministic."""
    assert extract_skills(DESCRIPTION, ["python"]) == extract_skills(DESCRIPTION, ["python"])
    assert extract_keywords(DESCRIPTION) == extract_keywords(DESCRIPTION)


def test_keywords_are_lowercase_and_unique():
    keywords = extract_keywords(DESCRIPTION)
    assert keywords and all(k == k.lower() for k in keywords) and len(keywords) == len(set(keywords))
    assert extract_keywords("   ") == []


def test_breakdown_by_headings():
    sections = breakdown(DESCRIPTION)
    assert sections["responsibilities"] == ["Design and run FastAPI services on Kubernetes", "Own our Kafka event pipeline"]
    assert sections["requirements"] == ["3+ years of experience with PostgreSQL", "Familiar with Docker and k8s"]
    assert sections["benefits"] == ["Remote-first, flexible hours", "Learning budget"]


def test_breakdown_by_keywords_without_headings():
    text = "You will build our data platform. Experience with Spark is required. We offer equity and remote work. Founded in 2019."
    sections = breakdown(text)
    assert sections["responsibilities"] == ["You will build our data platform."]
    assert sections["requirements"] == ["Experience with Spark is required."]
    assert sections["benefits"] == ["We offer equity and remote work."]  # "Founded in 2019." fits nowhere: left out
