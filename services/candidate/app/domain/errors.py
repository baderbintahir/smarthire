"""Candidate's business errors. Each has a stable code clients can switch on."""

from smarthire_common.errors import AppError, NotFoundError


class CandidateNotFoundError(NotFoundError):
    code = "CANDIDATE_NOT_FOUND"

    def __init__(self, message: str = "Candidate profile not found") -> None:
        super().__init__(message)


class FileTooLargeError(AppError):
    http_status = 413  # Content Too Large
    code = "FILE_TOO_LARGE"

    def __init__(self, limit_bytes: int) -> None:
        super().__init__(f"Resume files can be at most {limit_bytes // (1024 * 1024)} MB")


class UnsupportedFileTypeError(AppError):
    http_status = 415  # Unsupported Media Type
    code = "UNSUPPORTED_FILE_TYPE"

    def __init__(self) -> None:
        super().__init__("Upload a PDF or a Word (.docx) file")
