"""Identity's business errors. Each has a stable code clients can switch on."""

from smarthire_common.errors import ConflictError, ForbiddenError, NotFoundError, UnauthorizedError


class EmailAlreadyRegisteredError(ConflictError):
    code = "EMAIL_ALREADY_REGISTERED"

    def __init__(self) -> None:
        super().__init__("An account with this email already exists")


class InvalidCredentialsError(UnauthorizedError):
    code = "INVALID_CREDENTIALS"

    def __init__(self) -> None:
        # Deliberately generic: never say whether the email exists or the password was wrong
        super().__init__("Invalid email or password")


class InvalidRefreshTokenError(UnauthorizedError):
    code = "INVALID_REFRESH_TOKEN"

    def __init__(self) -> None:
        super().__init__("Session expired or invalid; please log in again")


class AccountSuspendedError(ForbiddenError):
    code = "ACCOUNT_SUSPENDED"

    def __init__(self) -> None:
        super().__init__("This account is suspended")


class RoleAlreadyHeldError(ConflictError):
    code = "ROLE_ALREADY_HELD"

    def __init__(self) -> None:
        super().__init__("The user already holds this role")


class RoleNotHeldError(NotFoundError):
    code = "ROLE_NOT_HELD"

    def __init__(self) -> None:
        super().__init__("The user does not hold this role")


class LastRoleError(ConflictError):
    code = "LAST_ROLE"

    def __init__(self) -> None:
        # A user with no roles would get tokens every service rejects; suspend the account instead
        super().__init__("A user must keep at least one role; suspend the account instead")
