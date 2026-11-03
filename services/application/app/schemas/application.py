import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

Reason = Annotated[str, StringConstraints(strip_whitespace=True, max_length=1000)]


class ApplyIn(BaseModel):
    job_id: uuid.UUID


class ApplicationOut(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    job_title: str
    candidate_id: uuid.UUID
    candidate_name: str
    status: str
    current_stage: str
    score: float | None
    score_status: str
    version: int  # send it back with recruiter edits (optimistic lock)
    created_at: datetime
    updated_at: datetime


class ApplicationPage(BaseModel):
    items: list[ApplicationOut]
    next_cursor: str | None


class BoardPage(BaseModel):
    items: list[ApplicationOut]


class StageEntryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    from_stage: str | None
    to_stage: str
    changed_by: uuid.UUID
    reason: str | None
    changed_at: datetime


class NoteOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    recruiter_id: uuid.UUID
    body: str
    created_at: datetime


class ApplicationDetail(ApplicationOut):
    history: list[StageEntryOut]
    notes: list[NoteOut] | None  # recruiters only: a candidate never sees recruiter notes


class StageChangeIn(BaseModel):
    to_stage: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
    version: int = Field(ge=1)  # the version the recruiter saw; stale -> 409
    reason: Reason | None = None


class DecisionIn(BaseModel):
    decision: Literal["HIRED", "REJECTED"]
    version: int = Field(ge=1)
    reason: Reason | None = None


class NoteIn(BaseModel):
    body: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=5000)]


def application_out(application, job) -> ApplicationOut:
    return ApplicationOut(
        id=application.id, job_id=application.job_id, job_title=job.title, candidate_id=application.candidate_id,
        candidate_name=application.candidate_name, status=application.status, current_stage=application.current_stage,
        score=application.score, score_status=application.score_status, version=application.version,
        created_at=application.created_at, updated_at=application.updated_at,
    )
