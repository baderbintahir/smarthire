"""Identity's business errors. Each has a stable code clients can switch on."""

from smarthire_common.errors import ConflictError, ForbiddenError, UnauthorizedError


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
