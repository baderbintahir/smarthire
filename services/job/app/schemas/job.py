import uuid
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, field_validator

Title = Annotated[str, StringConstraints(strip_whitespace=True, min_length=3, max_length=200)]
Skill = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=50)]
StageName = Annotated[str, StringConstraints(max_length=100)]


class EligibilityRules(BaseModel):
    """Checked by the Application service at apply time (Phase 8). Unknown keys are rejected so a
    typo can't silently create a rule nobody enforces."""

    model_config = ConfigDict(extra="forbid")
    min_experience_years: float | None = Field(default=None, ge=0, le=60)


class JobIn(BaseModel):
    title: Title
    description: str = Field(min_length=1, max_length=20_000)
    location: str | None = Field(default=None, max_length=200)
    employment_type: Literal["FULL_TIME", "PART_TIME", "CONTRACT", "INTERNSHIP"]
    required_skills: list[Skill] = Field(default_factory=list, max_length=50)
    eligibility_rules: EligibilityRules = Field(default_factory=EligibilityRules)
    application_limit: int | None = Field(default=None, ge=1)
    deadline: AwareDatetime | None = None  # timezone required: "2026-12-01T00:00:00Z"
    stages: list[StageName] = Field(min_length=1, max_length=20)  # the hiring pipeline, in order

    @field_validator("deadline")
    @classmethod
    def deadline_in_future(cls, value: datetime | None) -> datetime | None:
        if value is not None and value <= datetime.now(UTC):
            raise ValueError("deadline must be in the future")
        return value


class JobUpdate(JobIn):
    # Optimistic lock: the version the client read. If someone saved since, the update is a 409.
    version: int = Field(ge=1)


class StageOut(BaseModel):
    name: str
    position: int


class JobOut(BaseModel):
    id: uuid.UUID
    recruiter_id: uuid.UUID
    title: str
    description: str
    location: str | None
    employment_type: str
    required_skills: list[str]
    eligibility_rules: dict
    application_limit: int | None
    deadline: datetime | None
    status: str
    version: int
    published_at: datetime | None
    created_at: datetime
    updated_at: datetime
    stages: list[StageOut]


class JobPage(BaseModel):
    items: list[JobOut]
    next_cursor: str | None  # pass back as ?cursor= for the next page; null = last page


class PublishAccepted(BaseModel):
    id: uuid.UUID
    status: str
