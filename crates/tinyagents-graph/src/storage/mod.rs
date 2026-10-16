//! The graph crate's v2 storage declarations (stub).

use tinystoragedrivers_core::{Owner, Result, Schema, ScopedStorage};

#[must_use]
pub fn storage_schema() -> Schema {
    Schema::new(Owner::new("graph").expect("`graph` is a valid owner"))
}

pub async fn import_legacy_leases(_storage: &ScopedStorage) -> Result<u64> {
    Ok(0)
}

pub async fn import_legacy_task_cache(_storage: &ScopedStorage) -> Result<u64> {
    Ok(0)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
