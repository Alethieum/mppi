"""Pure provider-report validation; callers supply and own their payment state."""

from dataclasses import dataclass
from enum import StrEnum

from .amounts import Rates, TokenCounts, numerator, priced_numerator
from .errors import ValidationCode, ValidationError


class RunStatus(StrEnum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    NOT_ADMITTED = "NOT_ADMITTED"


@dataclass(frozen=True, slots=True)
class RunAccounting:
    counts: TokenCounts
    remaining_numerator: int
    final_status: RunStatus | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.counts, TokenCounts):
            raise ValidationError(ValidationCode.INVALID_VALUE, "counts must be TokenCounts")
        numerator(self.remaining_numerator, field="remaining reservation")
        if self.final_status is not None:
            if (
                not isinstance(self.final_status, RunStatus)
                or self.final_status is RunStatus.NOT_ADMITTED
            ):
                raise ValidationError(
                    ValidationCode.INVALID_VALUE, "admitted run requires a definitive final status"
                )
            if self.remaining_numerator != 0:
                raise ValidationError(
                    ValidationCode.INVALID_VALUE, "final accounting has no remaining reservation"
                )


@dataclass(frozen=True, slots=True)
class AccountingTransition:
    state: RunAccounting
    delta_numerator: int
    released_numerator: int
    duplicate: bool


def report_transition(
    previous: RunAccounting,
    counts: TokenCounts,
    rates: Rates,
    *,
    final_status: RunStatus | None = None,
) -> AccountingTransition:
    """Validate cumulative usage, conserve coverage and release only final slack.

    This returns a calculation, not an admission grant, a commit operation,
    proof of termination, or a new meter sequence. Runtime integration applies
    it to a consistent authoritative session view supplied by the provider.
    """
    if final_status is not None and (
        not isinstance(final_status, RunStatus) or final_status is RunStatus.NOT_ADMITTED
    ):
        raise ValidationError(ValidationCode.INVALID_VALUE, "invalid admitted-run final status")
    old_numerator = priced_numerator(previous.counts, rates)
    new_numerator = priced_numerator(counts, rates)
    if previous.final_status is not None:
        if counts == previous.counts and final_status == previous.final_status:
            return AccountingTransition(previous, 0, 0, True)
        raise ValidationError(
            ValidationCode.INCONSISTENT_USAGE, "report conflicts with definitive final accounting"
        )
    old_counts = previous.counts
    if (
        counts.input_tokens < old_counts.input_tokens
        or counts.cache_tokens < old_counts.cache_tokens
        or counts.output_tokens < old_counts.output_tokens
        or new_numerator < old_numerator
    ):
        raise ValidationError(
            ValidationCode.INCONSISTENT_USAGE,
            "cumulative counts or exact recognized cost regressed",
        )
    delta = new_numerator - old_numerator
    if delta > previous.remaining_numerator:
        raise ValidationError(
            ValidationCode.UNCOVERED_USAGE, "reported usage exceeds remaining funded reservation"
        )
    remaining = previous.remaining_numerator - delta
    released = remaining if final_status is not None else 0
    result = RunAccounting(counts, 0 if final_status is not None else remaining, final_status)
    return AccountingTransition(
        result, delta, released, counts == old_counts and final_status is None
    )
