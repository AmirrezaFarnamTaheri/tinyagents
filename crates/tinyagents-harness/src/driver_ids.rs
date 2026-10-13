//! Document ids for the driver-backed stores.

use sha2::{Digest, Sha256};

/// A document id for `parts`: a SHA-256 over the length-prefixed parts, in hex.
///
/// Length prefixes make the encoding injective, so `("a", "bc")` and
/// `("ab", "c")` never meet, and hashing keeps the id inside any driver's
/// length limit however long the parts are.
pub(crate) fn hashed_id(parts: &[&str]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
