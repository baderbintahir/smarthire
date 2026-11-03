"""The skills dictionary, shared (rule 8): Job Enrichment reads skills from job descriptions,
Candidate from resumes, and Scoring compares the two. All three must name skills the same way
("k8s" and "Kubernetes" are both "kubernetes"), or a perfect match would score zero.

Rule-based on purpose: the same text always gives the same answer (predictable retries and tests)."""

import re

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


def extract_skills(text: str, required_skills: list[str] = ()) -> list[str]:
    """The recruiter's required skills first (normalized), then dictionary skills found in the text.
    Sorted within each group, so the output never depends on dict or set order."""
    required = list(dict.fromkeys(normalize(s) for s in required_skills if s.strip()))
    found = sorted(skill for skill, patterns in _PATTERNS
                   if skill not in required and any(p.search(text) for p in patterns))
    return required + found
