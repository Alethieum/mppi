"""Local validation failures; never evidence of a chain transaction outcome."""

from enum import StrEnum


class ValidationCode(StrEnum):
    INVALID_VALUE = "INVALID_VALUE"
    LIMIT_EXCEEDED = "LIMIT_EXCEEDED"
    NON_CANONICAL = "NON_CANONICAL"
    DUPLICATE_KEY = "DUPLICATE_KEY"
    INCONSISTENT_USAGE = "INCONSISTENT_USAGE"
    UNCOVERED_USAGE = "UNCOVERED_USAGE"


class ValidationError(ValueError):
    """A rejected local input with a stable machine-readable code."""

    def __init__(self, code: ValidationCode, message: str) -> None:
        self.code = code
        super().__init__(message)
