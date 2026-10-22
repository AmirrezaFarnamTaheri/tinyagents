//! Stable JSON ordering for tool schemas rendered into prompt text.

use serde_json::Value;

/// Copies a value with recursively sorted object keys, including within arrays.
pub(crate) fn sorted(value: &Value) -> Value {
    let mut sorted = value.clone();
    sorted.sort_all_objects();
    sorted
}

/// Serializes sorted-key JSON using serde_json's compact format.
pub(crate) fn to_string(value: &Value) -> serde_json::Result<String> {
    serde_json::to_string(&sorted(value))
}
