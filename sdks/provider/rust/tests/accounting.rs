use alloy_primitives::U256;
use mppi_provider::accounting::{RunAccounting, RunStatus, report_transition};
use mppi_provider::amounts::{Rates, TokenCounts, priced_numerator};
use mppi_provider::errors::ValidationCode;
use serde_json::Value;

fn number(value: &Value) -> u64 {
    value.as_str().unwrap().parse().unwrap()
}

fn wide(value: &Value) -> U256 {
    value.as_str().unwrap().parse().unwrap()
}

fn counts(value: &Value) -> TokenCounts {
    TokenCounts::new(number(&value[0]), number(&value[1]), number(&value[2])).unwrap()
}

fn status(value: &Value) -> Option<RunStatus> {
    value.as_str().map(|value| match value {
        "COMPLETED" => RunStatus::Completed,
        "FAILED" => RunStatus::Failed,
        "CANCELLED" => RunStatus::Cancelled,
        "NOT_ADMITTED" => RunStatus::NotAdmitted,
        other => panic!("unknown fixture status {other}"),
    })
}

#[test]
fn shared_accounting_vectors_conserve_coverage() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../../testvectors/lifecycle.json")).unwrap();
    assert_eq!(fixture["schemaVersion"], 1);
    for case in fixture["cases"].as_array().unwrap() {
        let prices = &case["rates"];
        let prices = Rates::new(
            number(&prices["input"]),
            number(&prices["output"]),
            prices.get("cache").map(number),
        );
        let prior = &case["previous"];
        let previous = RunAccounting::new(
            counts(&prior["counts"]),
            wide(&prior["remainingNumerator"]),
            status(&prior["status"]),
        )
        .unwrap();
        let before = previous;
        let actual = report_transition(
            &previous,
            counts(&case["counts"]),
            prices,
            status(&case["status"]),
        );
        assert_eq!(previous, before);
        if let Some(error) = case.get("error") {
            assert_eq!(
                actual.unwrap_err().code().as_str(),
                error.as_str().unwrap(),
                "{}",
                case["name"]
            );
        } else {
            let result = actual.unwrap();
            let expected = &case["expect"];
            assert_eq!(
                result.delta_numerator(),
                wide(&expected["deltaNumerator"]),
                "{}",
                case["name"]
            );
            assert_eq!(
                result.released_numerator(),
                wide(&expected["releasedNumerator"]),
                "{}",
                case["name"]
            );
            assert_eq!(
                result.state().remaining_numerator(),
                wide(&expected["remainingNumerator"]),
                "{}",
                case["name"]
            );
            assert_eq!(
                result.duplicate(),
                expected["duplicate"].as_bool().unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(result.state().counts(), counts(&case["counts"]));
            assert_eq!(result.state().final_status(), status(&case["status"]));
            let initial = priced_numerator(previous.counts(), prices)
                .unwrap()
                .checked_add(previous.remaining_numerator())
                .unwrap();
            let final_coverage = priced_numerator(result.state().counts(), prices)
                .unwrap()
                .checked_add(result.state().remaining_numerator())
                .unwrap()
                .checked_add(result.released_numerator())
                .unwrap();
            assert_eq!(initial, final_coverage, "{}", case["name"]);
        }
    }
}

#[test]
fn admitted_accounting_rejects_invalid_final_state() {
    let counts = TokenCounts::new(0, 0, 0).unwrap();
    assert_eq!(
        RunAccounting::new(counts, U256::ZERO, Some(RunStatus::NotAdmitted))
            .unwrap_err()
            .code(),
        ValidationCode::InvalidValue
    );
    for status in [
        RunStatus::Completed,
        RunStatus::Failed,
        RunStatus::Cancelled,
    ] {
        assert_eq!(
            RunAccounting::new(counts, U256::ONE, Some(status))
                .unwrap_err()
                .code(),
            ValidationCode::InvalidValue
        );
        assert!(RunAccounting::new(counts, U256::ZERO, Some(status)).is_ok());
    }
    let state = RunAccounting::new(counts, U256::ONE, None).unwrap();
    assert_eq!(
        report_transition(
            &state,
            counts,
            Rates::new(0, 0, None),
            Some(RunStatus::NotAdmitted)
        )
        .unwrap_err()
        .code(),
        ValidationCode::InvalidValue
    );
    assert_eq!(state.remaining_numerator(), U256::ONE);
    assert_eq!(state.final_status(), None);
}

#[test]
fn subunit_cache_regression_cannot_hide_behind_rounding() {
    let prices = Rates::new(2, 0, Some(1));
    let state =
        RunAccounting::new(TokenCounts::new(1, 0, 0).unwrap(), U256::from(100), None).unwrap();
    let before = state;
    assert_eq!(
        report_transition(&state, TokenCounts::new(1, 1, 0).unwrap(), prices, None)
            .unwrap_err()
            .code(),
        ValidationCode::InconsistentUsage
    );
    assert_eq!(state, before);
}

#[test]
fn zero_prices_do_not_hide_counter_regression() {
    let counts = TokenCounts::new(2, 1, 2).unwrap();
    let state = RunAccounting::new(counts, U256::ZERO, None).unwrap();
    for next in [(1, 1, 2), (2, 0, 2), (2, 1, 1)] {
        assert_eq!(
            report_transition(
                &state,
                TokenCounts::new(next.0, next.1, next.2).unwrap(),
                Rates::new(0, 0, Some(0)),
                None
            )
            .unwrap_err()
            .code(),
            ValidationCode::InconsistentUsage
        );
    }
    let report = report_transition(
        &state,
        TokenCounts::new(3, 2, 3).unwrap(),
        Rates::new(0, 0, Some(0)),
        None,
    )
    .unwrap();
    assert!(!report.duplicate());
    assert_eq!(report.delta_numerator(), U256::ZERO);
    assert_eq!(report.released_numerator(), U256::ZERO);
}

#[test]
fn final_report_accounts_directly_and_exact_repeats_are_noops() {
    let prices = Rates::new(1_000_000, 1_000_000, None);
    for status in [
        RunStatus::Completed,
        RunStatus::Failed,
        RunStatus::Cancelled,
    ] {
        let state = RunAccounting::new(
            TokenCounts::new(0, 0, 0).unwrap(),
            U256::from(3_000_000),
            None,
        )
        .unwrap();
        let counts = TokenCounts::new(1, 0, 1).unwrap();
        let result = report_transition(&state, counts, prices, Some(status)).unwrap();
        assert_eq!(result.delta_numerator(), U256::from(2_000_000));
        assert_eq!(result.released_numerator(), U256::from(1_000_000));
        assert_eq!(result.state().remaining_numerator(), U256::ZERO);
        let repeat = report_transition(&result.state(), counts, prices, Some(status)).unwrap();
        assert!(repeat.duplicate());
        assert_eq!(repeat.state(), result.state());
        assert_eq!(repeat.delta_numerator(), U256::ZERO);
        assert_eq!(repeat.released_numerator(), U256::ZERO);
        assert_eq!(
            report_transition(&result.state(), counts, prices, None)
                .unwrap_err()
                .code(),
            ValidationCode::InconsistentUsage
        );
        assert_eq!(
            report_transition(
                &result.state(),
                TokenCounts::new(1, 0, 2).unwrap(),
                prices,
                Some(status)
            )
            .unwrap_err()
            .code(),
            ValidationCode::InconsistentUsage
        );
    }
}

#[test]
fn uint256_reservation_consumes_without_narrowing_or_early_release() {
    let prices = Rates::new(u64::MAX, u64::MAX, None);
    let start = RunAccounting::new(TokenCounts::new(0, 0, 0).unwrap(), U256::MAX, None).unwrap();
    let counts = TokenCounts::new(u64::MAX, 0, u64::MAX).unwrap();
    let result = report_transition(&start, counts, prices, None).unwrap();
    let cost = U256::from_limbs([2, u64::MAX - 3, 1, 0]);
    assert_eq!(result.delta_numerator(), cost);
    assert_eq!(
        result.state().remaining_numerator(),
        U256::MAX.checked_sub(cost).unwrap()
    );
    assert_eq!(result.released_numerator(), U256::ZERO);
    let terminal =
        report_transition(&result.state(), counts, prices, Some(RunStatus::Completed)).unwrap();
    assert_eq!(terminal.delta_numerator(), U256::ZERO);
    assert_eq!(
        terminal.released_numerator(),
        result.state().remaining_numerator()
    );
    assert_eq!(
        terminal.released_numerator().checked_add(cost).unwrap(),
        U256::MAX
    );
}

#[test]
fn over_reservation_final_never_releases_existing_coverage() {
    let state = RunAccounting::new(
        TokenCounts::new(0, 0, 0).unwrap(),
        U256::from(999_999),
        None,
    )
    .unwrap();
    let before = state;
    assert_eq!(
        report_transition(
            &state,
            TokenCounts::new(1, 0, 0).unwrap(),
            Rates::new(1_000_000, 1_000_000, None),
            Some(RunStatus::Failed)
        )
        .unwrap_err()
        .code(),
        ValidationCode::UncoveredUsage
    );
    assert_eq!(state, before);
}
