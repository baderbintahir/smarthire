"""Job's business errors. Each has a stable code clients can switch on."""

from smarthire_common.errors import AppError, ConflictError, NotFoundError


class JobTransitionError(ConflictError):
    """409: the job's current status doesn't allow this action (e.g. closing a DRAFT)."""

    code = "INVALID_JOB_TRANSITION"

    def __init__(self, current: str, target: str) -> None:
        super().__init__(f"A {current} job can't move to {target}")
        self.current, self.target = current, target


class JobNotEditableError(ConflictError):
    code = "JOB_NOT_EDITABLE"

    def __init__(self, status: str) -> None:
        super().__init__(f"A {status} job can't be edited")


class JobVersionConflictError(ConflictError):
    """409: optimistic lock. Someone saved the job after the client read it."""

    code = "VERSION_CONFLICT"

    def __init__(self) -> None:
        super().__init__("The job was changed by someone else; reload it and try again")


class InvalidJobStagesError(AppError):
    http_status = 422
    code = "INVALID_STAGES"


class JobNotFoundError(NotFoundError):
    code = "JOB_NOT_FOUND"

    def __init__(self) -> None:
        super().__init__("Job not found")


class StaleJobVersionError(ConflictError):
    """409 (design v2.1): a publishing step was asked to finish a version that is no longer current
    (edited or re-published since). The workflow stops quietly: a newer run owns the job now."""

    code = "STALE_JOB_VERSION"

    def __init__(self, job_id, version: int) -> None:
        super().__init__(f"Job {job_id} is no longer at version {version}")
