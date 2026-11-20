import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, EmailStr, Field, StringConstraints, field_validator, model_validator


# Roles a person may give themselves; ADMIN only via the CLI or an admin (design v1.1)
SelfRole = Literal["CANDIDATE", "RECRUITER"]
AnyRole = Literal["CANDIDATE", "RECRUITER", "ADMIN"]
FullName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
Company = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]


class RegisterRequest(BaseModel):
    email: EmailStr
    # max_length caps hashing cost: Argon2 on a 1 MB "password" would be a cheap DoS
    password: str = Field(min_length=8, max_length=128)
    full_name: FullName
    current_company: Company | None = None  # current employer, for any role
    # One account can hold several roles. ADMIN is deliberately not allowed here.
    roles: list[SelfRole] = Field(min_length=1, max_length=2)

    @field_validator("roles")
    @classmethod
    def no_duplicates(cls, roles: list[str]) -> list[str]:
        if len(set(roles)) != len(roles):
            raise ValueError("roles must not repeat")
        return roles


class ProfileUpdate(BaseModel):
    """PATCH /users/me: only the fields sent are changed. current_company: null clears it."""

    full_name: FullName | None = None
    current_company: Company | None = None

    @model_validator(mode="after")
    def something_to_change(self) -> "ProfileUpdate":
        if not self.model_fields_set:
            raise ValueError("send full_name and/or current_company")
        if "full_name" in self.model_fields_set and self.full_name is None:
            raise ValueError("full_name cannot be null")
        return self


class SelfRoleRequest(BaseModel):
    role: SelfRole


class AdminRoleRequest(BaseModel):
    role: AnyRole


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=128)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "Bearer"
    expires_in: int  # seconds until the access token expires


class UserOut(BaseModel):
    """What clients see about a user. Note: no password_hash, no token_version."""

    id: uuid.UUID
    email: str
    full_name: str
    current_company: str | None
    roles: list[str]
    status: str
    created_at: datetime
