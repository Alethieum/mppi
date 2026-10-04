import json
from pathlib import Path

import pytest
from mppi_protocol import (
    Rates,
    RunAccounting,
    RunStatus,
    TokenCounts,
    ValidationError,
    priced_numerator,
    report_transition,
)

VECTORS = Path(__file__).resolve().parents[3] / "testvectors"


def test_shared_lifecycle_vectors() -> None:
    fixture = json.loads((VECTORS / "lifecycle.json").read_text())
    for case in fixture["cases"]:
        rates = Rates(**{k: int(v) for k, v in case["rates"].items()})
        previous = case["previous"]
        counts = TokenCounts(*(int(n) for n in previous["counts"]))
        status = RunStatus(previous["status"]) if previous["status"] is not None else None
        state = RunAccounting(counts, int(previous["remainingNumerator"]), status)
        new_counts = TokenCounts(*(int(n) for n in case["counts"]))
        final_status = RunStatus(case["status"]) if case["status"] is not None else None
        if "error" in case:
            with pytest.raises(ValidationError) as caught:
                report_transition(state, new_counts, rates, final_status=final_status)
            assert caught.value.code.value == case["error"], case["name"]
            assert state.counts == counts
            assert state.remaining_numerator == int(previous["remainingNumerator"])
        else:
            transition = report_transition(state, new_counts, rates, final_status=final_status)
            expect = case["expect"]
            assert str(transition.delta_numerator) == expect["deltaNumerator"], case["name"]
            assert str(transition.released_numerator) == expect["releasedNumerator"], case["name"]
            assert str(transition.state.remaining_numerator) == expect["remainingNumerator"], case[
                "name"
            ]
            assert transition.duplicate is expect["duplicate"], case["name"]
            before = priced_numerator(state.counts, rates) + state.remaining_numerator
            after = (
                priced_numerator(transition.state.counts, rates)
                + transition.state.remaining_numerator
            )
            assert before == after + transition.released_numerator


def test_subunit_cache_regression_cannot_hide_behind_floor() -> None:
    rates = Rates(2, 0, 1)
    state = RunAccounting(TokenCounts(1, 0, 0), 100)
    with pytest.raises(ValidationError, match="regressed"):
        report_transition(state, TokenCounts(1, 1, 0), rates)


def test_terminal_final_count_does_not_require_duplicate_usage_message() -> None:
    state = RunAccounting(TokenCounts(0, 0, 0), 3000000)
    final = report_transition(
        state, TokenCounts(1, 0, 1), Rates(1000000, 1000000), final_status=RunStatus.CANCELLED
    )
    assert final.delta_numerator == 2000000
    assert final.released_numerator == 1000000
    repeat = report_transition(
        final.state, final.state.counts, Rates(1000000, 1000000), final_status=RunStatus.CANCELLED
    )
    assert repeat.duplicate
    with pytest.raises(ValidationError):
        report_transition(
            final.state,
            final.state.counts,
            Rates(1000000, 1000000),
            final_status=RunStatus.COMPLETED,
        )
