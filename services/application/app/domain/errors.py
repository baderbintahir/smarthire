"""Application's business errors. Each has a stable code clients can switch on."""

from smarthire_common.errors import AppError, ConflictError, NotFoundError


class _Unprocessable(AppError):
    http_status = 422  # the request is well-formed, but a business rule says no


class JobNotOpenError(_Unprocessable):
    code = "JOB_NOT_OPEN"


class ProfileIncompleteError(_Unprocessable):
    code = "PROFILE_INCOMPLETE"


class NotEligibleError(_Unprocessable):
    code = "NOT_ELIGIBLE"

    def __init__(self, reasons: list[str]) -> None:
        super().__init__("You don't meet this job's requirements", details={"reasons": reasons})


class QuotaExceededError(_Unprocessable):
    code = "QUOTA_EXCEEDED"

    def __init__(self, limit: int) -> None:
        super().__init__(f"You can have at most {limit} open applications; withdraw one to apply")


class InvalidStageError(_Unprocessable):
    code = "INVALID_STAGE"


class IdempotencyKeyReusedError(_Unprocessable):
    code = "IDEMPOTENCY_KEY_REUSED"

    def __init__(self) -> None:
        super().__init__("This Idempotency-Key was already used for a different request")


class AlreadyAppliedError(ConflictError):
    code = "ALREADY_APPLIED"

    def __init__(self) -> None:
        super().__init__("You have already applied to this job")


class ApplicationNotActiveError(ConflictError):
    code = "APPLICATION_NOT_ACTIVE"

    def __init__(self, status: str) -> None:
        super().__init__(f"The application is {status}; it can't change anymore")


class VersionConflictError(ConflictError):
    code = "VERSION_CONFLICT"

    def __init__(self) -> None:
        super().__init__("The application was changed by someone else; reload it and try again")


class IdempotencyInProgressError(ConflictError):
    code = "IDEMPOTENCY_IN_PROGRESS"

    def __init__(self) -> None:
        super().__init__("A request with this Idempotency-Key is still being processed; retry shortly")


class ApplicationNotFoundError(NotFoundError):
    code = "APPLICATION_NOT_FOUND"

    def __init__(self) -> None:
        super().__init__("Application not found")
