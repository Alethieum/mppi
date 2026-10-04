//! MPPI's bounded ASCII catalog profile and exact catalog commitments.
//!
//! Publication sorts declarations; parsing accepts only already canonical bytes.
//! Every price is a canonical decimal string. This is not a general JCS encoder.

use std::{
    cell::Cell,
    collections::{BTreeMap, btree_map::Entry},
    fmt,
};

use alloy_primitives::{B256, keccak256};
use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};

use crate::{
    amounts::{Rates, decimal_uint64},
    errors::{ValidationCode, ValidationError},
};

pub const DEFAULT_CATALOG_BYTES: usize = 1 << 20;
pub const DEFAULT_JSON_DEPTH: usize = 128;

fn invalid(message: &'static str) -> ValidationError {
    ValidationError::new(ValidationCode::InvalidValue, message)
}

fn size_error() -> ValidationError {
    ValidationError::new(ValidationCode::LimitExceeded, "catalog byte limit exceeded")
}

/// Validate the protocol's case-sensitive namespace/repository model grammar.
pub fn validate_model(model: &str) -> Result<&str, ValidationError> {
    fn endpoint(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || byte == b'_'
    }
    fn component(value: &str) -> bool {
        let bytes = value.as_bytes();
        bytes.first().is_some_and(|&byte| endpoint(byte))
            && bytes.last().is_some_and(|&byte| endpoint(byte))
            && bytes
                .iter()
                .all(|&byte| endpoint(byte) || matches!(byte, b'.' | b'-'))
    }

    let Some((namespace, repository)) = model.split_once('/') else {
        return Err(invalid("model must be namespace/repository"));
    };
    if !component(namespace)
        || !component(repository)
        || repository.len() > 96
        || model.contains("--")
        || model.contains("..")
        || model.ends_with(".git")
    {
        return Err(invalid("model does not match the MPPI identifier profile"));
    }
    Ok(model)
}

/// One validated declaration. A missing cache price differs from a zero price.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogRow {
    model: String,
    rates: Rates,
}

impl CatalogRow {
    pub fn new(model: impl Into<String>, rates: Rates) -> Result<Self, ValidationError> {
        let model = model.into();
        validate_model(&model)?;
        Ok(Self { model, rates })
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn rates(&self) -> &Rates {
        &self.rates
    }
}

/// Validated rows and their immutable, exact-byte Keccak commitment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Catalog {
    rows: Vec<CatalogRow>,
    canonical_bytes: Vec<u8>,
    rate_card_hash: B256,
}

impl Catalog {
    pub fn rows(&self) -> &[CatalogRow] {
        &self.rows
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub fn rate_card_hash(&self) -> B256 {
        self.rate_card_hash
    }

    pub fn rates_for(&self, model: &str) -> Option<&Rates> {
        self.rows
            .binary_search_by(|row| row.model.as_str().cmp(model))
            .ok()
            .map(|index| &self.rows[index].rates)
    }
}

fn check_limit(max_bytes: usize) -> Result<(), ValidationError> {
    if !(1..=DEFAULT_CATALOG_BYTES).contains(&max_bytes) {
        return Err(invalid("catalog limit must be in 1..1048576 bytes"));
    }
    Ok(())
}

fn check_depth(raw: &[u8]) -> Result<(), ValidationError> {
    let mut depth = 0_usize;
    let mut quoted = false;
    let mut escaped = false;
    for &byte in raw {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else if byte == b'"' {
            quoted = true;
        } else if matches!(byte, b'[' | b'{') {
            depth += 1;
            if depth > DEFAULT_JSON_DEPTH {
                return Err(ValidationError::new(
                    ValidationCode::LimitExceeded,
                    "JSON nesting limit exceeded",
                ));
            }
        } else if matches!(byte, b']' | b'}') {
            // The JSON parser rejects unmatched closing delimiters.
            depth = depth.saturating_sub(1);
        }
    }
    Ok(())
}

// Only strings, arrays and objects are needed by the catalog schema. The visitor
// still traverses every container so unknown fields cannot hide duplicate keys.
enum Json {
    String(String),
    Array(Vec<Json>),
    Object(BTreeMap<String, Json>),
    Other,
}

struct JsonSeed<'a>(&'a Cell<bool>);

impl<'de> DeserializeSeed<'de> for JsonSeed<'_> {
    type Value = Json;

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Json, D::Error> {
        deserializer.deserialize_any(JsonVisitor(self.0))
    }
}

struct JsonVisitor<'a>(&'a Cell<bool>);

impl<'de> Visitor<'de> for JsonVisitor<'_> {
    type Value = Json;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("valid JSON")
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Json, E> {
        Ok(Json::String(value.to_owned()))
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Json, E> {
        Ok(Json::String(value))
    }

    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Json, E> {
        Ok(Json::Other)
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Json, E> {
        Ok(Json::Other)
    }

    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Json, E> {
        Ok(Json::Other)
    }

    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Json, E> {
        Ok(Json::Other)
    }

    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Json, E> {
        Ok(Json::Other)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Json, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(JsonSeed(self.0))? {
            values.push(value);
        }
        Ok(Json::Array(values))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Json, A::Error> {
        let mut values = BTreeMap::new();
        while let Some(key) = map.next_key::<String>()? {
            match values.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(map.next_value_seed(JsonSeed(self.0))?);
                }
                Entry::Occupied(_) => {
                    self.0.set(true);
                    return Err(A::Error::custom("duplicate decoded JSON property"));
                }
            }
        }
        Ok(Json::Object(values))
    }
}

fn decode(raw: &[u8]) -> Result<Json, ValidationError> {
    let text = std::str::from_utf8(raw).map_err(|_| invalid("catalog is not valid UTF-8 JSON"))?;
    let duplicate = Cell::new(false);
    let mut deserializer = serde_json::Deserializer::from_str(text);
    // serde_json counts its recursion budget differently. The bounded precheck
    // already enforced MPPI's exact 128-container limit before any JSON allocation.
    deserializer.disable_recursion_limit();
    let result = JsonSeed(&duplicate).deserialize(&mut deserializer);
    let value = result.map_err(|_| {
        if duplicate.get() {
            ValidationError::new(
                ValidationCode::DuplicateKey,
                "duplicate decoded JSON property",
            )
        } else {
            invalid("catalog is not valid UTF-8 JSON")
        }
    })?;
    deserializer
        .end()
        .map_err(|_| invalid("catalog is not valid UTF-8 JSON"))?;
    Ok(value)
}

fn take_string(row: &mut BTreeMap<String, Json>, key: &str) -> Result<String, ValidationError> {
    match row.remove(key) {
        Some(Json::String(value)) => Ok(value),
        _ => Err(invalid("catalog fields must be strings")),
    }
}

fn row_size(row: &CatalogRow) -> Result<usize, ValidationError> {
    let mut size = b"{\"input\":\"\",\"model\":\"\",\"output\":\"\"}".len();
    for extra in [
        row.model.len(),
        row.rates.input().to_string().len(),
        row.rates.output().to_string().len(),
    ] {
        size = size.checked_add(extra).ok_or_else(size_error)?;
    }
    if let Some(cache) = row.rates.cache() {
        size = size
            .checked_add(b"\"cache\":\"\",".len() + cache.to_string().len())
            .ok_or_else(size_error)?;
    }
    Ok(size)
}

fn encode(rows: &[CatalogRow], capacity: usize) -> Vec<u8> {
    let mut raw = Vec::with_capacity(capacity);
    raw.push(b'[');
    for (index, row) in rows.iter().enumerate() {
        if index != 0 {
            raw.push(b',');
        }
        raw.push(b'{');
        if let Some(cache) = row.rates.cache() {
            raw.extend_from_slice(b"\"cache\":\"");
            raw.extend_from_slice(cache.to_string().as_bytes());
            raw.extend_from_slice(b"\",");
        }
        raw.extend_from_slice(b"\"input\":\"");
        raw.extend_from_slice(row.rates.input().to_string().as_bytes());
        raw.extend_from_slice(b"\",\"model\":\"");
        raw.extend_from_slice(row.model.as_bytes());
        raw.extend_from_slice(b"\",\"output\":\"");
        raw.extend_from_slice(row.rates.output().to_string().as_bytes());
        raw.extend_from_slice(b"\"}");
    }
    raw.push(b']');
    raw
}

/// Validate received bytes without repairing order, whitespace or spelling.
pub fn parse_catalog(raw: &[u8], max_bytes: usize) -> Result<Catalog, ValidationError> {
    check_limit(max_bytes)?;
    if raw.len() > max_bytes {
        return Err(size_error());
    }
    check_depth(raw)?;
    let Json::Array(items) = decode(raw)? else {
        return Err(invalid("catalog must be a nonempty array"));
    };
    if items.is_empty() {
        return Err(invalid("catalog must be a nonempty array"));
    }
    let mut rows: Vec<CatalogRow> = Vec::new();
    for item in items {
        let Json::Object(mut fields) = item else {
            return Err(invalid("invalid catalog row fields"));
        };
        if !fields.contains_key("model")
            || !fields.contains_key("input")
            || !fields.contains_key("output")
            || fields.len() != 3 + usize::from(fields.contains_key("cache"))
        {
            return Err(invalid("invalid catalog row fields"));
        }
        let model = take_string(&mut fields, "model")?;
        validate_model(&model)?;
        if rows.last().is_some_and(|previous| previous.model >= model) {
            return Err(ValidationError::new(
                ValidationCode::NonCanonical,
                "catalog model order must be strictly increasing",
            ));
        }
        let input = decimal_uint64(&take_string(&mut fields, "input")?)?;
        let output = decimal_uint64(&take_string(&mut fields, "output")?)?;
        let cache = if fields.contains_key("cache") {
            Some(decimal_uint64(&take_string(&mut fields, "cache")?)?)
        } else {
            None
        };
        rows.push(CatalogRow {
            model,
            rates: Rates::new(input, output, cache),
        });
    }
    let canonical_bytes = encode(&rows, raw.len());
    if canonical_bytes != raw {
        return Err(ValidationError::new(
            ValidationCode::NonCanonical,
            "catalog bytes differ from canonical encoding",
        ));
    }
    Ok(Catalog {
        rows,
        canonical_bytes,
        rate_card_hash: keccak256(raw),
    })
}

/// Sort bounded provider declarations and emit canonical catalog bytes.
pub fn publish_catalog(
    rows: impl IntoIterator<Item = CatalogRow>,
    max_bytes: usize,
) -> Result<Catalog, ValidationError> {
    check_limit(max_bytes)?;
    let mut bounded = Vec::new();
    let mut encoded_size = 2_usize;
    for row in rows {
        encoded_size = encoded_size
            .checked_add(row_size(&row)?)
            .and_then(|size| size.checked_add(usize::from(!bounded.is_empty())))
            .ok_or_else(size_error)?;
        if encoded_size > max_bytes {
            return Err(size_error());
        }
        bounded.push(row);
    }
    bounded.sort_unstable_by(|left, right| left.model.cmp(&right.model));
    if bounded.is_empty()
        || bounded
            .windows(2)
            .any(|pair| pair[0].model == pair[1].model)
    {
        return Err(invalid("catalog must contain unique nonempty model rows"));
    }
    let canonical_bytes = encode(&bounded, encoded_size);
    Ok(Catalog {
        rows: bounded,
        rate_card_hash: keccak256(&canonical_bytes),
        canonical_bytes,
    })
}
