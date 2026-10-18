//! Compatibility exports for compact tool argument signatures.
//!
//! The renderer is owned by `tinytools-agent` beside its prompt dialects. These
//! wrappers keep the harness's established API while delegating all rendering
//! to that implementation.

use serde_json::Value;

/// Nesting past this depth renders as a bare `object` / `unknown[]`.
pub const MAX_DEPTH: usize = 4;
/// Properties past this count in one object render as `…`.
pub const MAX_PROPERTIES: usize = 16;
/// A rendered signature is clipped to this many characters.
pub const MAX_SIGNATURE_CHARS: usize = 300;
/// Longest per-argument note kept by [`argument_notes`].
pub const MAX_NOTE_CHARS: usize = 100;

/// Renders `schema` as a TypeScript-style type, clipped to
/// [`MAX_SIGNATURE_CHARS`].
#[must_use]
pub fn type_signature(schema: &Value) -> String {
    tinytools_agent::render::compact_type_signature(schema)
}

/// One `name: first sentence` line per top-level property that carries a
/// description, in sorted key order independent of serde_json features.
/// Empty when none does.
#[must_use]
pub fn argument_notes(schema: &Value) -> Vec<String> {
    tinytools_agent::render::argument_notes(schema)
}

#[cfg(test)]
#[path = "signature_tests.rs"]
mod test;
