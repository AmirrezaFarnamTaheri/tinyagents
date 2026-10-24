//! Parse-once utility for embedded JSON tool schemas.

use serde_json::Value;

/// Parse an embedded schema and identify the source location on malformed JSON.
pub fn parse_embedded(site: &str, source: &str) -> Value {
    serde_json::from_str(source)
        .unwrap_or_else(|error| panic!("embedded tool schema at {site} is not valid JSON: {error}"))
}

/// Parse an embedded JSON schema once and return a clone at each call site.
#[macro_export]
macro_rules! static_schema {
    ($source:expr) => {{
        static SCHEMA: ::std::sync::LazyLock<::serde_json::Value> =
            ::std::sync::LazyLock::new(|| {
                $crate::schema::parse_embedded(concat!(file!(), ":", line!()), $source)
            });
        ::std::clone::Clone::clone(&*SCHEMA)
    }};
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
