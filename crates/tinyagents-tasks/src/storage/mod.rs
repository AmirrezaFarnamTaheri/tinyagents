//! The tasks crate's v2 storage declarations.

use tinystoragedrivers_core::{Owner, Schema};

/// The tasks crate's storage declarations.
#[must_use]
pub fn storage_schema() -> Schema {
    Schema::new(Owner::new("tasks").expect("`tasks` is a valid owner"))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
