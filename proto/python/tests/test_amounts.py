import json
from fractions import Fraction
from pathlib import Path

import pytest
from mppi_protocol import (
    MAX_UINT64,
    Rates,
    TokenCounts,
    ValidationError,
    cumulative_usage,
    decimal_uint64,
    funding_amount,
    priced_numerator,
    remaining_funding_tokens,
    required_coverage,
    top_up_total,
    uint64,
)

VECTORS = Path(__file__).resolve().parents[3] / "testvectors"


def test_shared_amount_vectors() -> None:
    fixture = json.loads((VECTORS / "amounts.json").read_text())
    assert fixture["schemaVersion"] == 1
    for case in fixture["cases"]:
        arguments = case["input"]

        def run(case=case, arguments=arguments) -> int | None:
            match case["operation"]:
                case "funding":
                    rates = Rates(**{k: int(v) for k, v in arguments["rates"].items()})
                    return funding_amount(int(arguments["tokens"]), rates)
                case "usage":
                    return cumulative_usage(int(n) for n in arguments["numerators"])
                case "coverage":
                    return required_coverage(int(n) for n in arguments["numerators"])
                case "top_up":
                    return top_up_total(int(arguments["deposit"]), int(arguments["amount"]))
                case "remaining":
                    rates = Rates(**{k: int(v) for k, v in arguments["rates"].items()})
                    return remaining_funding_tokens(
                        int(arguments["deposit"]),
                        int(arguments["usageNumerator"]),
                        (int(n) for n in arguments["reservations"]),
                        rates,
                    )
                case _:
                    raise AssertionError(case["operation"])

        if "error" in case:
            with pytest.raises(ValidationError) as caught:
                run()
            assert caught.value.code.value == case["error"], case["name"]
        else:
            actual = run()
            assert (None if actual is None else str(actual)) == case["expect"], case["name"]


@pytest.mark.parametrize("value", [True, False, -1, 1.0, "1", None, MAX_UINT64 + 1])
def test_uint64_rejects_coercion(value: object) -> None:
    with pytest.raises(ValidationError):
        uint64(value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value", ["01", "+1", "-0", "1e0", "1.0", " 1", "1 ", "١", "", str(MAX_UINT64 + 1), "9" * 10000]
)
def test_decimal_rejects_noncanonical(value: str) -> None:
    with pytest.raises(ValidationError):
        decimal_uint64(value)


def test_exact_fraction_oracle_and_funding_bound() -> None:
    for input_rate in (0, 1, 999999, 1000000, MAX_UINT64):
        for output_rate in (0, 3, 1600000):
            for cache_rate in (None, 0, 2000000):
                rates = Rates(input_rate, output_rate, cache_rate)
                for input_count, cache_count, output_count in ((0, 0, 0), (1, 0, 2), (7, 3, 9)):
                    if cache_count and cache_rate is None:
                        continue
                    counts = TokenCounts(input_count, cache_count, output_count)
                    exact = Fraction(
                        (input_count - cache_count) * input_rate + output_count * output_rate,
                        1000000,
                    )
                    if cache_rate is not None:
                        exact += Fraction(cache_count * cache_rate, 1000000)
                    value = priced_numerator(counts, rates)
                    assert Fraction(value, 1000000) == exact
                    assert cumulative_usage((value,)) == exact.numerator // exact.denominator
                    assert required_coverage((value,)) == -(-exact.numerator // exact.denominator)
                    assert value <= (input_count + output_count) * rates.greatest


def test_missing_cache_price_is_not_free_or_input_fallback() -> None:
    with pytest.raises(ValidationError, match="no accepted cache price"):
        priced_numerator(TokenCounts(1, 1, 0), Rates(1000000, 1000000))
    assert priced_numerator(TokenCounts(1, 1, 0), Rates(1000000, 1000000, 0)) == 0


def test_wide_numerator_valid_final_amount() -> None:
    wide = MAX_UINT64 * 1000000
    assert wide > MAX_UINT64
    assert cumulative_usage((wide,)) == MAX_UINT64
    assert required_coverage((wide,)) == MAX_UINT64
    with pytest.raises(ValidationError):
        required_coverage((wide, 1))
