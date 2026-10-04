use std::cell::Cell;

use alloy_primitives::{B256, hex};
use mppi_provider::{
    amounts::Rates,
    catalog::{
        CatalogRow, DEFAULT_CATALOG_BYTES, DEFAULT_JSON_DEPTH, parse_catalog, publish_catalog,
        validate_model,
    },
    errors::ValidationCode,
};
use serde_json::Value;

const BASIC: &[u8] = br#"[{"input":"1","model":"x/y","output":"2"}]"#;

fn row(model: &str, rates: Rates) -> CatalogRow {
    CatalogRow::new(model, rates).unwrap()
}

#[test]
fn shared_catalog_vectors_preserve_bytes_hashes_and_failure_codes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../../testvectors/catalog.json")).unwrap();
    assert_eq!(fixture["schemaVersion"], 1);
    for case in fixture["accept"].as_array().unwrap() {
        let raw = case["utf8"].as_str().unwrap().as_bytes();
        let catalog = parse_catalog(raw, DEFAULT_CATALOG_BYTES).unwrap();
        assert_eq!(catalog.canonical_bytes(), raw, "{}", case["name"]);
        assert_eq!(
            catalog.rate_card_hash(),
            case["keccak256"].as_str().unwrap().parse::<B256>().unwrap(),
            "{}",
            case["name"]
        );
        assert_eq!(
            publish_catalog(catalog.rows().iter().rev().cloned(), DEFAULT_CATALOG_BYTES).unwrap(),
            catalog,
            "{}",
            case["name"]
        );
    }
    for case in fixture["reject"].as_array().unwrap() {
        let raw = hex::decode(case["rawHex"].as_str().unwrap()).unwrap();
        let error = parse_catalog(&raw, DEFAULT_CATALOG_BYTES).unwrap_err();
        assert_eq!(
            error.code().as_str(),
            case["error"].as_str().unwrap(),
            "{}",
            case["name"]
        );
    }
}

#[test]
fn publication_sorts_and_lookup_preserves_case_and_optional_cache() {
    let catalog = publish_catalog(
        [
            row("a/model", Rates::new(1, 2, Some(0))),
            row("A/model", Rates::new(3, 4, None)),
        ],
        DEFAULT_CATALOG_BYTES,
    )
    .unwrap();
    assert_eq!(catalog.rows()[0].model(), "A/model");
    assert_eq!(catalog.rows()[1].model(), "a/model");
    assert_eq!(catalog.rates_for("A/model"), Some(&Rates::new(3, 4, None)));
    assert_eq!(
        catalog.rates_for("a/model"),
        Some(&Rates::new(1, 2, Some(0)))
    );
    assert_eq!(catalog.rates_for("A/MODEL"), None);
    assert_eq!(catalog.rows()[0].rates().cache(), None);
    assert_eq!(catalog.rows()[1].rates().cache(), Some(0));

    let absent =
        publish_catalog([row("x/y", Rates::new(1, 2, None))], DEFAULT_CATALOG_BYTES).unwrap();
    let explicit = publish_catalog(
        [row("x/y", Rates::new(1, 2, Some(0)))],
        DEFAULT_CATALOG_BYTES,
    )
    .unwrap();
    assert_ne!(absent.rate_card_hash(), explicit.rate_card_hash());
}

#[test]
fn publication_enforces_exact_byte_budgets() {
    let cases = [
        (
            Rates::new(0, u64::MAX, None),
            br#"[{"input":"0","model":"x/y","output":"18446744073709551615"}]"#.as_slice(),
        ),
        (
            Rates::new(u64::MAX, 1, Some(0)),
            br#"[{"cache":"0","input":"18446744073709551615","model":"x/y","output":"1"}]"#
                .as_slice(),
        ),
        (
            Rates::new(1, 0, Some(u64::MAX)),
            br#"[{"cache":"18446744073709551615","input":"1","model":"x/y","output":"0"}]"#
                .as_slice(),
        ),
    ];
    for (rates, expected) in cases {
        let catalog = publish_catalog([row("x/y", rates)], expected.len()).unwrap();
        assert_eq!(catalog.canonical_bytes(), expected);
        assert_eq!(parse_catalog(expected, expected.len()).unwrap(), catalog);
        assert_eq!(
            publish_catalog([row("x/y", rates)], expected.len() - 1)
                .unwrap_err()
                .code(),
            ValidationCode::LimitExceeded
        );
        assert_eq!(
            parse_catalog(expected, expected.len() - 1)
                .unwrap_err()
                .code(),
            ValidationCode::LimitExceeded
        );
    }

    let rows = [
        row("z/m", Rates::new(1, 2, None)),
        row("a/m", Rates::new(3, 4, Some(0))),
    ];
    let expected = br#"[{"cache":"0","input":"3","model":"a/m","output":"4"},{"input":"1","model":"z/m","output":"2"}]"#;
    assert_eq!(
        publish_catalog(rows.clone(), expected.len())
            .unwrap()
            .canonical_bytes(),
        expected
    );
    assert_eq!(
        publish_catalog(rows, expected.len() - 1)
            .unwrap_err()
            .code(),
        ValidationCode::LimitExceeded
    );
}

#[test]
fn publication_stops_consuming_as_soon_as_a_row_exceeds_the_budget() {
    let consumed = Cell::new(0);
    let rows = std::iter::repeat_with(|| {
        consumed.set(consumed.get() + 1);
        assert_eq!(
            consumed.get(),
            1,
            "must stop before requesting another declaration"
        );
        row("x/y", Rates::new(1, 2, None))
    });
    assert_eq!(
        publish_catalog(rows, BASIC.len() - 1).unwrap_err().code(),
        ValidationCode::LimitExceeded
    );
    assert_eq!(consumed.get(), 1);

    let oversized = CatalogRow::new(
        "a".repeat(DEFAULT_CATALOG_BYTES) + "/b",
        Rates::new(1, 1, None),
    )
    .unwrap();
    assert_eq!(
        publish_catalog([oversized], DEFAULT_CATALOG_BYTES)
            .unwrap_err()
            .code(),
        ValidationCode::LimitExceeded
    );
}

#[test]
fn empty_and_duplicate_publication_is_rejected() {
    assert_eq!(
        publish_catalog([], DEFAULT_CATALOG_BYTES)
            .unwrap_err()
            .code(),
        ValidationCode::InvalidValue
    );
    let repeated = row("x/y", Rates::new(1, 2, None));
    assert_eq!(
        publish_catalog([repeated.clone(), repeated], DEFAULT_CATALOG_BYTES)
            .unwrap_err()
            .code(),
        ValidationCode::InvalidValue
    );
}

#[test]
fn decoded_duplicates_are_rejected_in_every_container() {
    let cases: &[&[u8]] = &[
        br#"[{"input":"1","model":"x/y","output":"2","extra":{"x":0,"\u0078":1}}]"#,
        br#"[{"input":"1","model":"x/y","output":"2","cache":[{"a":null,"a":true}]}]"#,
        br#"{"$serde_json::private::RawValue":"[]","x":0,"x":1}"#,
        br#"{"$serde_json::private::Number":"1","x":0,"x":1}"#,
    ];
    for raw in cases {
        assert_eq!(
            parse_catalog(raw, DEFAULT_CATALOG_BYTES)
                .unwrap_err()
                .code(),
            ValidationCode::DuplicateKey
        );
    }

    let raw = "[".repeat(DEFAULT_JSON_DEPTH - 1)
        + r#"{"x":0,"\u0078":1}"#
        + &"]".repeat(DEFAULT_JSON_DEPTH - 1);
    assert_eq!(
        parse_catalog(raw.as_bytes(), DEFAULT_CATALOG_BYTES)
            .unwrap_err()
            .code(),
        ValidationCode::DuplicateKey
    );
}

#[test]
fn limits_precede_json_decoding_and_depth_allocation() {
    for limit in [0, DEFAULT_CATALOG_BYTES + 1, usize::MAX] {
        assert_eq!(
            parse_catalog(BASIC, limit).unwrap_err().code(),
            ValidationCode::InvalidValue
        );
        assert_eq!(
            publish_catalog([row("x/y", Rates::new(1, 2, None))], limit)
                .unwrap_err()
                .code(),
            ValidationCode::InvalidValue
        );
    }
    assert_eq!(
        parse_catalog(&[0xff; 9], 8).unwrap_err().code(),
        ValidationCode::LimitExceeded
    );
    assert_eq!(
        parse_catalog(&[0xff], 8).unwrap_err().code(),
        ValidationCode::InvalidValue
    );
    let permitted_depth = "[".repeat(DEFAULT_JSON_DEPTH) + &"]".repeat(DEFAULT_JSON_DEPTH);
    assert_eq!(
        parse_catalog(permitted_depth.as_bytes(), DEFAULT_CATALOG_BYTES)
            .unwrap_err()
            .code(),
        ValidationCode::InvalidValue
    );
    let raw = "[".repeat(DEFAULT_JSON_DEPTH + 1) + &"]".repeat(DEFAULT_JSON_DEPTH + 1);
    assert_eq!(
        parse_catalog(raw.as_bytes(), DEFAULT_CATALOG_BYTES)
            .unwrap_err()
            .code(),
        ValidationCode::LimitExceeded
    );
    let quoted = format!(
        r#"[{{"input":"{}","model":"x/y","output":"2"}}]"#,
        "[".repeat(129)
    );
    assert_eq!(
        parse_catalog(quoted.as_bytes(), DEFAULT_CATALOG_BYTES)
            .unwrap_err()
            .code(),
        ValidationCode::InvalidValue
    );
}

#[test]
fn malformed_strings_non_json_numbers_and_trailing_documents_are_rejected() {
    let cases: &[&[u8]] = &[
        br#"[{"input":"1","model":"x/y","output":"2","cache":null}]"#,
        br#"[{"input":true,"model":"x/y","output":"2"}]"#,
        br#"[{"input":1e400,"model":"x/y","output":"2"}]"#,
        br#"[{"input":Infinity,"model":"x/y","output":"2"}]"#,
        br#"[{"input":"1","model":"x/y","output":"2","extra":"\ud800"}]"#,
        br#"[{"input":"1","model":"x/y","output":"2","\udfff":0}]"#,
        br#"[{"input":"1","model":"x/y","output":"2"}]{}"#,
    ];
    for raw in cases {
        assert_eq!(
            parse_catalog(raw, DEFAULT_CATALOG_BYTES)
                .unwrap_err()
                .code(),
            ValidationCode::InvalidValue
        );
    }
}

#[test]
fn model_validation_preserves_the_exact_ascii_profile() {
    for valid in ["_/_", "A_1/B-c.d_e", "a/model.GIT"] {
        assert_eq!(validate_model(valid).unwrap(), valid);
    }
    for invalid in [
        "a/b/c", "a//b", "a/.b", "a/b-", "a--b/c", "a..b/c", "a/b.git", "a/b\n", "a/é",
    ] {
        assert_eq!(
            validate_model(invalid).unwrap_err().code(),
            ValidationCode::InvalidValue
        );
    }
    assert!(validate_model(&("n".repeat(200) + "/" + &"r".repeat(96))).is_ok());
    assert!(validate_model(&("n/".to_owned() + &"r".repeat(97))).is_err());
}
