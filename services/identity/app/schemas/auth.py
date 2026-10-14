import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, EmailStr, Field, model_validator


class RegisterRequest(BaseModel):
    email: EmailStr
    # max_length caps hashing cost: Argon2 on a 1 MB "password" would be a cheap DoS
    password: str = Field(min_length=8, max_length=128)
    # ADMIN is deliberately not allowed here: admins are created with `python -m app.cli`
    role: Literal["CANDIDATE", "RECRUITER"]
    display_name: str | None = Field(default=None, min_length=1, max_length=100)
    company: str | None = Field(default=None, max_length=200)

    @model_validator(mode="after")
    def recruiter_fields(self) -> "RegisterRequest":
        if self.role == "RECRUITER" and not self.display_name:
            raise ValueError("display_name is required for recruiters")
        if self.role == "CANDIDATE" and (self.display_name or self.company):
            raise ValueError("display_name and company are only for recruiters")
        return self


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=128)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "Bearer"
    expires_in: int  # seconds until the access token expires


class RecruiterProfileOut(BaseModel):
    display_name: str
    company: str | None


class UserOut(BaseModel):
    """What clients see about a user. Note: no password_hash, no token_version."""

    id: uuid.UUID
    email: str
    role: str
    status: str
    created_at: datetime
    recruiter_profile: RecruiterProfileOut | None = None
