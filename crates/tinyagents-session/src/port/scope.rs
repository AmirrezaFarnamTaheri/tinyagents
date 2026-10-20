//! Session-owned construction of the storage scopes used by this crate.
//!
//! `tinystoragedrivers-types::Scope` is deliberately opaque and validates only
//! serialized identifiers. This module preserves the session namespace
//! conventions formerly supplied by the engine-bearing TSD core.

use sha2::{Digest, Sha256};
use tinystoragedrivers_types::{Scope, StorageError};

const MAX_SEGMENT_LEN: usize = 96;
const HASH_CHARS: usize = 26;

/// The single-operator tenant scope.
pub fn local_scope() -> Scope {
    Scope::new("local").expect("static local scope is valid")
}

/// A user tenant in the stable `u:<segment>` form.
///
/// # Errors
/// Returns `InvalidInput` when `id` is empty or the serialized scope exceeds
/// the shared scope limit.
pub fn user_scope(id: &str) -> Result<Scope, StorageError> {
    Ok(Scope::new(format!("u:{}", encode_segment(id)?))?)
}

/// Add an agent principal under a validated local or user tenant.
///
/// # Errors
/// Returns `InvalidInput` for an empty id, non-tenant input, or overlong
/// serialized scope.
pub fn agent_scope(tenant: &Scope, id: &str) -> Result<Scope, StorageError> {
    let tenant_text = tenant.as_str();
    if tenant_text != "local"
        && (!tenant_text.starts_with("u:") || !valid_segment(&tenant_text[2..]))
    {
        return Err(StorageError::invalid_input(
            "agent scope requires a local or user tenant without a principal",
        ));
    }
    Scope::new(format!("{tenant_text}/agent:{}", encode_segment(id)?))
}

fn encode_segment(id: &str) -> Result<String, StorageError> {
    if id.is_empty() {
        return Err(StorageError::invalid_input("scope id must not be empty"));
    }
    if is_plain_segment(id) {
        return Ok(id.to_owned());
    }
    let mut encoded = data_encoding::BASE32_NOPAD
        .encode(&Sha256::digest(id.as_bytes()))
        .to_ascii_lowercase();
    encoded.truncate(HASH_CHARS);
    Ok(format!("~{encoded}"))
}

fn is_plain_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment.len() <= MAX_SEGMENT_LEN
        && segment.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'@')
        })
}

fn valid_segment(segment: &str) -> bool {
    is_plain_segment(segment)
        || segment.strip_prefix('~').is_some_and(|hash| {
            hash.len() == HASH_CHARS
                && hash.bytes().all(|byte| matches!(byte, b'a'..=b'z' | b'2'..=b'7'))
        })
}

#[cfg(test)]
#[path = "scope_tests.rs"]
mod tests;
