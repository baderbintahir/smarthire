import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
Skill = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=50)]
Year = Annotated[int, Field(ge=1950, le=2100)]


class EducationEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")  # a mistyped key is a 422, not silently stored
    institution: Name
    degree: Name
    field: str | None = Field(default=None, max_length=200)
    start_year: Year | None = None
    end_year: Year | None = None

    @model_validator(mode="after")
    def years_in_order(self):
        if self.start_year and self.end_year and self.end_year < self.start_year:
            raise ValueError("end_year must not be before start_year")
        return self


class ProfileIn(BaseModel):
    """PUT = the whole profile (optional fields left out are cleared, not kept)."""

    full_name: Name
    headline: str | None = Field(default=None, max_length=200)
    skills: list[Skill] = Field(default_factory=list, max_length=50)
    # Decimal, not float: 2.1 years must stay exactly 2.1 (the column is numeric(4,1))
    experience_years: Decimal | None = Field(default=None, ge=0, le=80, decimal_places=1)
    education: list[EducationEntry] = Field(default_factory=list, max_length=20)


class ResumeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)  # build it from the ORM object
    id: uuid.UUID
    file_name: str
    content_type: str
    size_bytes: int
    status: str
    is_active: bool
    uploaded_at: datetime
    # Never the object key or a bucket URL: the storage layout is internal


class ProfileOut(BaseModel):
    id: uuid.UUID
    full_name: str
    headline: str | None
    skills: list[str]
    experience_years: float | None
    education: list[EducationEntry]
    active_resume: ResumeOut | None
    created_at: datetime
    updated_at: datetime


class ResumeList(BaseModel):
    items: list[ResumeOut]
