from datetime import datetime

from pydantic import BaseModel


class Sections(BaseModel):
    requirements: list[str]
    responsibilities: list[str]
    benefits: list[str]


class StructuredJobOut(BaseModel):
    job_id: str
    version: int
    skills: list[str]
    required_skills: list[str] = []  # the recruiter's must-haves (a subset of skills); Scoring weighs these more
    keywords: list[str]
    sections: Sections
    created_at: datetime
    relevance: float | None = None  # search only: how well it matched

    @classmethod
    def from_doc(cls, doc: dict) -> "StructuredJobOut":
        return cls(**{k: v for k, v in doc.items() if k != "_id"})


class SearchResults(BaseModel):
    items: list[StructuredJobOut]
