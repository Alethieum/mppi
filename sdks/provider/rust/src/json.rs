//! Bounded strict JSON for integration documents and unchanged inference bytes.
//!
//! RawValue delegates syntax to serde_json; each decoded object key is checked
//! before insertion. Scalars are decoded separately, so arbitrary_precision's
//! private number marker can never turn a caller's object into a number.

use crate::{ValidationCode, ValidationError};
use serde::de::{DeserializeSeed, Error as _, MapAccess, Visitor};
use serde_json::{Value, value::RawValue};
use std::{cell::Cell, collections::BTreeMap, fmt};

fn invalid() -> ValidationError {
    ValidationError::new(ValidationCode::InvalidValue, "invalid JSON document")
}

pub(crate) fn check_depth(raw: &[u8], maximum: usize) -> Result<(), ValidationError> {
    let (mut depth, mut quoted, mut escaped) = (0usize, false, false);
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
            if depth > maximum {
                return Err(ValidationError::new(
                    ValidationCode::LimitExceeded,
                    "JSON nesting limit exceeded",
                ));
            }
        } else if matches!(byte, b']' | b'}') {
            depth = depth.saturating_sub(1);
        }
    }
    Ok(())
}

struct ObjectSeed<'a>(&'a Cell<bool>);
impl<'de> DeserializeSeed<'de> for ObjectSeed<'_> {
    type Value = BTreeMap<String, &'de RawValue>;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for ObjectSeed<'_> {
    type Value = BTreeMap<String, &'de RawValue>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON object")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut values = BTreeMap::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.contains_key(&key) {
                self.0.set(true);
                return Err(A::Error::custom("duplicate decoded JSON property"));
            }
            values.insert(key, map.next_value::<&RawValue>()?);
        }
        Ok(values)
    }
}

fn materialize(raw: &RawValue) -> Result<Value, ValidationError> {
    let source = raw.get();
    match source.as_bytes().first() {
        Some(b'{') => {
            let duplicate = Cell::new(false);
            let mut deserializer = serde_json::Deserializer::from_str(source);
            deserializer.disable_recursion_limit();
            let object = ObjectSeed(&duplicate)
                .deserialize(&mut deserializer)
                .map_err(|_| {
                    if duplicate.get() {
                        ValidationError::new(
                            ValidationCode::DuplicateKey,
                            "duplicate decoded JSON property",
                        )
                    } else {
                        invalid()
                    }
                })?;
            deserializer.end().map_err(|_| invalid())?;
            let mut values = serde_json::Map::new();
            for (key, value) in object {
                values.insert(key, materialize(value)?);
            }
            Ok(Value::Object(values))
        }
        Some(b'[') => {
            let values: Vec<&RawValue> = serde_json::from_str(source).map_err(|_| invalid())?;
            values
                .into_iter()
                .map(materialize)
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array)
        }
        Some(b'"') => serde_json::from_str::<String>(source)
            .map(Value::String)
            .map_err(|_| invalid()),
        Some(b't' | b'f') => serde_json::from_str::<bool>(source)
            .map(Value::Bool)
            .map_err(|_| invalid()),
        Some(b'n') => Ok(Value::Null),
        Some(b'-' | b'0'..=b'9') => serde_json::from_str::<serde_json::Number>(source)
            .map(Value::Number)
            .map_err(|_| invalid()),
        _ => Err(invalid()),
    }
}

/// Parse strict UTF-8 JSON without rounding numeric values. This decoded tree
/// is for validation; inference integrations must retain the original bytes.
pub fn parse_document(
    raw: &[u8],
    max_bytes: usize,
    max_depth: usize,
) -> Result<Value, ValidationError> {
    if max_bytes == 0 || !(1..=crate::DEFAULT_JSON_DEPTH).contains(&max_depth) {
        return Err(ValidationError::new(
            ValidationCode::InvalidValue,
            "invalid JSON limits",
        ));
    }
    if raw.len() > max_bytes {
        return Err(ValidationError::new(
            ValidationCode::LimitExceeded,
            "JSON byte limit exceeded",
        ));
    }
    check_depth(raw, max_depth)?;
    // Validate all UTF-8 before borrowing a RawValue (which skips string contents).
    std::str::from_utf8(raw).map_err(|_| invalid())?;
    let value: &RawValue = serde_json::from_slice(raw).map_err(|_| invalid())?;
    materialize(value)
}
