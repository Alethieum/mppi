//! Pure payment calculations using provider-supplied counts and accepted prices.

use mppi_provider::{
    CatalogRow, DEFAULT_CATALOG_BYTES, Rates, RunAccounting, RunStatus, TokenCounts, U256,
    ValidationError, cumulative_usage, parse_catalog, priced_numerator, publish_catalog,
    report_transition, required_coverage,
};

fn main() -> Result<(), ValidationError> {
    let catalog = publish_catalog(
        [CatalogRow::new(
            "example/model",
            Rates::new(1_000_000, 2_000_000, None),
        )?],
        DEFAULT_CATALOG_BYTES,
    )?;
    let accepted = parse_catalog(catalog.canonical_bytes(), DEFAULT_CATALOG_BYTES)?;
    let rates = *accepted
        .rates_for("example/model")
        .expect("published model");
    let bound = priced_numerator(TokenCounts::new(100, 0, 1_000)?, rates)?;
    let ceiling = required_coverage([bound])?;
    let previous = RunAccounting::new(TokenCounts::new(0, 0, 0)?, bound, None)?;
    let report = report_transition(
        &previous,
        TokenCounts::new(100, 0, 10)?,
        rates,
        Some(RunStatus::Completed),
    )?;
    let usage = cumulative_usage([report.delta_numerator()])?;

    assert_eq!(ceiling, 2_100);
    assert_eq!(usage, 120);
    assert_eq!(report.state().remaining_numerator(), U256::ZERO);
    assert_eq!(
        report
            .delta_numerator()
            .checked_add(report.released_numerator()),
        Some(bound)
    );
    println!("Coverage ceiling: {ceiling}; actual usage: {usage} raw USDC units");
    Ok(())
}
