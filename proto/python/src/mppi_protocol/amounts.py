"""Exact MPPI amounts. All monetary outputs are six-decimal USDC base units."""

import re
from collections.abc import Iterable
from dataclasses import dataclass

from .errors import ValidationCode, ValidationError

MAX_UINT64 = (1 << 64) - 1
RATE_SCALE = 1_000_000
_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)\Z", re.ASCII)


def uint64(value: int, *, field: str = "value", positive: bool = False) -> int:
    """Reject booleans, coercions and out-of-range integers before encoding."""
    minimum = 1 if positive else 0
    if type(value) is not int or not minimum <= value <= MAX_UINT64:
        raise ValidationError(
            ValidationCode.INVALID_VALUE, f"{field} must be a uint64 integer >= {minimum}"
        )
    return value


def decimal_uint64(value: str, *, field: str = "value") -> int:
    """Parse the exact catalog/TOML decimal-string grammar without float use."""
    if type(value) is not str or len(value) > 20 or _DECIMAL.fullmatch(value) is None:
        raise ValidationError(
            ValidationCode.INVALID_VALUE, f"{field} must be a canonical decimal string"
        )
    return uint64(int(value), field=field)


def numerator(value: int, *, field: str = "numerator") -> int:
    """Validate a wide nonnegative integer; no uint64 intermediate truncation."""
    if type(value) is not int or value < 0:
        raise ValidationError(
            ValidationCode.INVALID_VALUE, f"{field} must be a nonnegative integer"
        )
    return value


@dataclass(frozen=True, slots=True)
class Rates:
    input: int
    output: int
    cache: int | None = None

    def __post_init__(self) -> None:
        uint64(self.input, field="input rate")
        uint64(self.output, field="output rate")
        if self.cache is not None:
            uint64(self.cache, field="cache rate")

    @property
    def greatest(self) -> int:
        return max(self.input, self.output, *(() if self.cache is None else (self.cache,)))


@dataclass(frozen=True, slots=True)
class TokenCounts:
    """Provider-supplied cumulative counts; cached input is a subset of input."""

    input_tokens: int
    cache_tokens: int
    output_tokens: int

    def __post_init__(self) -> None:
        uint64(self.input_tokens, field="input_tokens")
        uint64(self.cache_tokens, field="cache_tokens")
        uint64(self.output_tokens, field="output_tokens")
        if self.cache_tokens > self.input_tokens:
            raise ValidationError(ValidationCode.INVALID_VALUE, "cache_tokens exceeds input_tokens")


def priced_numerator(counts: TokenCounts, rates: Rates) -> int:
    """Price supplied counts without rounding or adding a missing cache rate."""
    if counts.cache_tokens and rates.cache is None:
        raise ValidationError(
            ValidationCode.INCONSISTENT_USAGE, "positive cache count has no accepted cache price"
        )
    return (
        (counts.input_tokens - counts.cache_tokens) * rates.input
        + (counts.cache_tokens * rates.cache if rates.cache is not None else 0)
        + counts.output_tokens * rates.output
    )


def cumulative_usage(numerators: Iterable[int]) -> int:
    """One floor after summing every accepted run/model/version numerator."""
    return uint64(sum(numerator(n) for n in numerators) // RATE_SCALE, field="cumulative usage")


def required_coverage(numerators: Iterable[int]) -> int:
    """One ceiling over usage and all remaining/new bounds, never per row."""
    total = sum(numerator(n) for n in numerators)
    whole, remainder = divmod(total, RATE_SCALE)
    return uint64(whole + bool(remainder), field="required coverage")


def funding_amount(tokens: int, rates: Rates) -> int:
    """Caller-chosen positive funding tokens at the greatest accepted rate."""
    uint64(tokens, field="funding tokens", positive=True)
    amount = required_coverage((tokens * rates.greatest,))
    return uint64(amount, field="funding amount", positive=True)


def top_up_total(deposit: int, amount: int) -> int:
    """Validate positive raw-unit funding and cumulative deposit capacity."""
    uint64(deposit, field="deposit", positive=True)
    uint64(amount, field="top-up amount", positive=True)
    return uint64(deposit + amount, field="cumulative deposit", positive=True)


def remaining_funding_tokens(
    deposit: int, usage_numerator: int, reservation_numerators: Iterable[int], rates: Rates
) -> int | None:
    """Advisory deposit headroom; all-zero pricing has no token estimate."""
    uint64(deposit, field="deposit")
    numerator(usage_numerator)
    reserved = sum(numerator(n) for n in reservation_numerators)
    if rates.greatest == 0:
        return None
    available = max(0, deposit * RATE_SCALE - usage_numerator - reserved)
    return min(MAX_UINT64, available // rates.greatest)
