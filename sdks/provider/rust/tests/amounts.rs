use alloy_primitives::U256;
use mppi_provider::amounts::{
    MAX_UINT64, RATE_SCALE, Rates, TokenCounts, cumulative_usage, decimal_uint64, funding_amount,
    priced_numerator, remaining_funding_tokens, required_coverage, top_up_total,
};
use mppi_provider::errors::{ValidationCode, ValidationError};
use serde_json::Value;

fn number(value: &Value) -> u64 {
    value.as_str().unwrap().parse().unwrap()
}

fn wide(value: &Value) -> U256 {
    value.as_str().unwrap().parse().unwrap()
}

fn numerators(value: &Value) -> Vec<U256> {
    value.as_array().unwrap().iter().map(wide).collect()
}

fn rates(value: &Value) -> Rates {
    Rates::new(
        number(&value["input"]),
        number(&value["output"]),
        value.get("cache").map(number),
    )
}

#[test]
fn public_units_and_accepted_rates_are_exact() {
    assert_eq!(MAX_UINT64, u64::MAX);
    assert_eq!(RATE_SCALE, 1_000_000);
    let prices = Rates::new(1, 2, Some(3));
    assert_eq!(prices.input(), 1);
    assert_eq!(prices.output(), 2);
    assert_eq!(prices.cache(), Some(3));
    assert_eq!(prices.greatest(), 3);
    assert_eq!(Rates::new(3, 2, None).greatest(), 3);
    assert_eq!(Rates::new(1, 2, None).greatest(), 2);
}

#[test]
fn shared_amount_vectors() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../../testvectors/amounts.json")).unwrap();
    assert_eq!(fixture["schemaVersion"], 1);
    for case in fixture["cases"].as_array().unwrap() {
        let input = &case["input"];
        let actual: Result<Option<u64>, ValidationError> = match case["operation"].as_str().unwrap()
        {
            "funding" => funding_amount(number(&input["tokens"]), rates(&input["rates"])).map(Some),
            "usage" => cumulative_usage(numerators(&input["numerators"])).map(Some),
            "coverage" => required_coverage(numerators(&input["numerators"])).map(Some),
            "top_up" => top_up_total(number(&input["deposit"]), number(&input["amount"])).map(Some),
            "remaining" => remaining_funding_tokens(
                number(&input["deposit"]),
                wide(&input["usageNumerator"]),
                numerators(&input["reservations"]),
                rates(&input["rates"]),
            ),
            operation => panic!("unknown fixture operation {operation}"),
        };
        if let Some(error) = case.get("error") {
            assert_eq!(
                actual.unwrap_err().code().as_str(),
                error.as_str().unwrap(),
                "{}",
                case["name"]
            );
        } else {
            let actual = actual.unwrap().map(|value| value.to_string());
            assert_eq!(
                actual.as_deref(),
                case["expect"].as_str(),
                "{}",
                case["name"]
            );
        }
    }
}

#[test]
fn decimal_grammar_and_uint64_boundary() {
    assert_eq!(decimal_uint64("0").unwrap(), 0);
    assert_eq!(decimal_uint64("18446744073709551615").unwrap(), u64::MAX);
    for value in [
        "",
        "01",
        "+1",
        "-1",
        "-0",
        "1e0",
        "1.0",
        " 1",
        "1 ",
        "١",
        "18446744073709551616",
    ] {
        assert_eq!(
            decimal_uint64(value).unwrap_err().code(),
            ValidationCode::InvalidValue,
            "{value}"
        );
    }
    assert_eq!(
        decimal_uint64(&"9".repeat(10_000)).unwrap_err().code(),
        ValidationCode::InvalidValue
    );
}

#[test]
fn counts_validate_cache_subset() {
    assert_eq!(
        TokenCounts::new(0, 1, 0).unwrap_err().code(),
        ValidationCode::InvalidValue
    );
    let counts = TokenCounts::new(u64::MAX, u64::MAX, u64::MAX).unwrap();
    assert_eq!(counts.input_tokens(), u64::MAX);
    assert_eq!(counts.cache_tokens(), u64::MAX);
    assert_eq!(counts.output_tokens(), u64::MAX);
}

#[test]
fn missing_cache_price_does_not_become_free_or_input_price() {
    let cached = TokenCounts::new(1, 1, 0).unwrap();
    assert_eq!(
        priced_numerator(cached, Rates::new(1_000_000, 1_000_000, None))
            .unwrap_err()
            .code(),
        ValidationCode::InconsistentUsage,
    );
    assert_eq!(
        priced_numerator(cached, Rates::new(1_000_000, 1_000_000, Some(0))).unwrap(),
        U256::ZERO
    );
    assert_eq!(
        priced_numerator(TokenCounts::new(1, 0, 0).unwrap(), Rates::new(3, 0, None)).unwrap(),
        U256::from(3),
    );
}

#[test]
fn maximum_products_need_more_than_uint128() {
    let counts = TokenCounts::new(u64::MAX, 0, u64::MAX).unwrap();
    let prices = Rates::new(u64::MAX, u64::MAX, None);
    // 2 * (2^64 - 1)^2 = 2^129 - 2^66 + 2, written independently as limbs.
    let expected = U256::from_limbs([2, u64::MAX - 3, 1, 0]);
    let actual = priced_numerator(counts, prices).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        cumulative_usage([actual]).unwrap_err().code(),
        ValidationCode::InvalidValue
    );
    assert_eq!(
        priced_numerator(
            TokenCounts::new(u64::MAX, u64::MAX, u64::MAX).unwrap(),
            Rates::new(0, u64::MAX, Some(u64::MAX)),
        )
        .unwrap(),
        expected,
    );
}

#[test]
fn exact_integer_oracle_covers_price_combinations() {
    for input_rate in [0, 1, 999_999, 1_000_000, u64::MAX] {
        for output_rate in [0, 3, 1_600_000] {
            for cache_rate in [None, Some(0), Some(2_000_000)] {
                let prices = Rates::new(input_rate, output_rate, cache_rate);
                for (input, cache, output) in [(0, 0, 0), (1, 0, 2), (7, 3, 9)] {
                    if cache > 0 && cache_rate.is_none() {
                        continue;
                    }
                    let expected = u128::from(input - cache) * u128::from(input_rate)
                        + u128::from(cache) * u128::from(cache_rate.unwrap_or(0))
                        + u128::from(output) * u128::from(output_rate);
                    let actual =
                        priced_numerator(TokenCounts::new(input, cache, output).unwrap(), prices)
                            .unwrap();
                    assert_eq!(actual, U256::from(expected));
                    assert_eq!(
                        u128::from(cumulative_usage([actual]).unwrap()),
                        expected / u128::from(RATE_SCALE)
                    );
                    assert_eq!(
                        u128::from(required_coverage([actual]).unwrap()),
                        expected.div_ceil(u128::from(RATE_SCALE))
                    );
                }
            }
        }
    }
}

#[test]
fn aggregate_rounding_and_final_uint64_boundary() {
    assert_eq!(cumulative_usage([]).unwrap(), 0);
    assert_eq!(required_coverage([]).unwrap(), 0);
    assert_eq!(
        cumulative_usage([U256::from(600_000), U256::from(600_000)]).unwrap(),
        1
    );
    assert_eq!(
        required_coverage([U256::from(400_000), U256::from(400_000)]).unwrap(),
        1
    );
    let maximum = U256::from(u64::MAX)
        .checked_mul(U256::from(RATE_SCALE))
        .unwrap();
    assert_eq!(cumulative_usage([maximum]).unwrap(), u64::MAX);
    assert_eq!(required_coverage([maximum]).unwrap(), u64::MAX);
    assert_eq!(cumulative_usage([maximum, U256::ONE]).unwrap(), u64::MAX);
    assert_eq!(
        required_coverage([maximum, U256::ONE]).unwrap_err().code(),
        ValidationCode::InvalidValue
    );
    assert_eq!(
        cumulative_usage([maximum, U256::from(RATE_SCALE)])
            .unwrap_err()
            .code(),
        ValidationCode::InvalidValue
    );
}

#[test]
fn wide_aggregation_overflow_rejects_instead_of_wrapping() {
    for values in [[U256::MAX, U256::ONE], [U256::MAX, U256::MAX]] {
        assert_eq!(
            cumulative_usage(values).unwrap_err().code(),
            ValidationCode::InvalidValue
        );
        assert_eq!(
            required_coverage(values).unwrap_err().code(),
            ValidationCode::InvalidValue
        );
    }
    // A quotient/remainder ceiling must not wrap MAX + Q - 1 into small money.
    assert_eq!(
        required_coverage([U256::MAX]).unwrap_err().code(),
        ValidationCode::InvalidValue
    );
}

#[test]
fn funding_and_top_up_remain_positive_and_checked() {
    assert_eq!(
        funding_amount(RATE_SCALE, Rates::new(0, 1, Some(u64::MAX))).unwrap(),
        u64::MAX
    );
    assert_eq!(
        funding_amount(u64::MAX, Rates::new(1, 1, None)).unwrap(),
        18_446_744_073_710
    );
    assert_eq!(
        funding_amount(0, Rates::new(1, 1, None))
            .unwrap_err()
            .code(),
        ValidationCode::InvalidValue
    );
    assert_eq!(
        funding_amount(1, Rates::new(0, 0, Some(0)))
            .unwrap_err()
            .code(),
        ValidationCode::InvalidValue
    );
    assert_eq!(top_up_total(u64::MAX - 1, 1).unwrap(), u64::MAX);
    for (deposit, amount) in [(0, 1), (1, 0), (u64::MAX, 1), (1, u64::MAX)] {
        assert_eq!(
            top_up_total(deposit, amount).unwrap_err().code(),
            ValidationCode::InvalidValue
        );
    }
}

#[test]
fn guidance_subtracts_usage_and_reservations_once() {
    let prices = Rates::new(1_000_000, 2_000_000, None);
    assert_eq!(
        remaining_funding_tokens(10, U256::from(1_600_000), [U256::from(1_600_000)], prices)
            .unwrap(),
        Some(3)
    );
    assert_eq!(
        remaining_funding_tokens(0, U256::ONE, [], prices).unwrap(),
        Some(0)
    );
    assert_eq!(
        remaining_funding_tokens(1, U256::from(1_000_001), [], prices).unwrap(),
        Some(0)
    );
    assert_eq!(
        remaining_funding_tokens(u64::MAX, U256::ZERO, [], Rates::new(1, 0, None)).unwrap(),
        Some(u64::MAX)
    );
    assert_eq!(
        remaining_funding_tokens(1, U256::ZERO, [], Rates::new(0, 0, None)).unwrap(),
        None
    );
}

#[test]
fn guidance_wide_overflow_is_an_error() {
    let prices = Rates::new(1, 0, None);
    assert_eq!(
        remaining_funding_tokens(1, U256::ZERO, [U256::MAX, U256::ONE], prices)
            .unwrap_err()
            .code(),
        ValidationCode::InvalidValue
    );
    assert_eq!(
        remaining_funding_tokens(1, U256::MAX, [U256::ONE], prices)
            .unwrap_err()
            .code(),
        ValidationCode::InvalidValue
    );
}
