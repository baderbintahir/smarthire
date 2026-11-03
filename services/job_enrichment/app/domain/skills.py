"""Skill and keyword extraction (pure functions, no I/O).

Rule-based on purpose: a fixed dictionary gives the SAME answer every time for the same text, which
keeps Temporal retries and tests predictable. Semantic matching comes later (Phase 15)."""

import re

import yake

# canonical skill -> other ways people write it (all compared lowercase, as whole words)
SKILLS: dict[str, tuple[str, ...]] = {
    "python": (), "java": (), "javascript": ("js",), "typescript": ("ts",), "go": ("golang",),
    "rust": (), "c#": ("csharp",), "c++": ("cpp",), "kotlin": (), "ruby": (), "php": (), "scala": (),
    "sql": (), "postgresql": ("postgres", "psql"), "mysql": (), "mongodb": ("mongo",), "redis": (),
    "elasticsearch": ("elastic search",), "kafka": ("apache kafka",), "rabbitmq": (), "celery": (),
    "temporal": (), "fastapi": (), "django": (), "flask": (), "spring": ("spring boot",), "node.js": ("nodejs", "node"),
    "react": ("reactjs", "react.js"), "angular": (), "vue": ("vue.js", "vuejs"), "graphql": (), "rest": ("restful",),
    "grpc": (), "docker": (), "kubernetes": ("k8s",), "terraform": (), "ansible": (), "aws": ("amazon web services",),
    "gcp": ("google cloud",), "azure": (), "linux": (), "git": (), "ci/cd": ("cicd", "continuous integration"),
    "microservices": ("microservice",), "machine learning": ("ml",), "pandas": (), "spark": ("apache spark",),
    "airflow": (), "llm": ("llms", "large language models"), "opentelemetry": ("otel",), "prometheus": (),
    "grafana": (), "pytest": (), "html": (), "css": (),
}


def _pattern(term: str) -> re.Pattern:
    # Whole words only ("go" must not match "good"); \b doesn't work next to symbols like "#"/"+"
    return re.compile(rf"(?<![\w+#.]){re.escape(term)}(?![\w+#])", re.IGNORECASE)


_PATTERNS = [(skill, [_pattern(t) for t in (skill, *aliases)]) for skill, aliases in SKILLS.items()]


def normalize(skill: str) -> str:
    """Map an alias to its canonical name ("k8s" -> "kubernetes"); unknown skills stay as typed (lowercase)."""
    lowered = skill.strip().lower()
    for canonical, aliases in SKILLS.items():
        if lowered == canonical or lowered in aliases:
            return canonical
    return lowered


def extract_skills(description: str, required_skills: list[str]) -> list[str]:
    """The recruiter's required skills first (normalized), then dictionary skills found in the text.
    Sorted within each group, so the output never depends on dict or set order."""
    required = list(dict.fromkeys(normalize(s) for s in required_skills if s.strip()))
    found = sorted(skill for skill, patterns in _PATTERNS
                   if skill not in required and any(p.search(description) for p in patterns))
    return required + found


def extract_keywords(text: str, top: int = 10) -> list[str]:
    """Phrases that stand out in this text (YAKE: statistics of the text itself, no model, no network).
    Lowercased and de-duplicated, best first."""
    if not text.strip():
        return []
    extractor = yake.KeywordExtractor(lan="en", n=2, top=top, dedupLim=0.7)
    keywords = [kw.lower() for kw, _score in extractor.extract_keywords(text)]
    return list(dict.fromkeys(keywords))
