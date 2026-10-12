//! Stable JSON object ordering at fingerprint and prompt-text boundaries.
//!
//! Cargo feature unification can enable serde_json/preserve_order through a
//! downstream dependency such as BSON. Explicit recursive sorting preserves
//! the bytes produced by serde_json's default sorted maps without mutating
//! callers' values. Array order and scalar serialization remain unchanged.
//! This is sorted-key JSON, not RFC 8785 numeric canonicalization.

use serde_json::Value;

/// Copies a value with recursively sorted object keys, including within arrays.
pub(crate) fn sorted(value: &Value) -> Value {
    let mut sorted = value.clone();
    sorted.sort_all_objects();
    sorted
}

/// Serializes sorted-key JSON using serde_json's ordinary compact format.
pub(crate) fn to_string(value: &Value) -> serde_json::Result<String> {
    serde_json::to_string(&sorted(value))
}

/// Serializes sorted-key JSON using serde_json's ordinary byte encoding.
pub(crate) fn to_vec(value: &Value) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&sorted(value))
}

/// Serializes sorted-key JSON using serde_json's ordinary pretty format.
pub(crate) fn to_string_pretty(value: &Value) -> serde_json::Result<String> {
    serde_json::to_string_pretty(&sorted(value))
}

#[cfg(test)]
#[path = "canonical_json_tests.rs"]
mod tests;
