//! Stable description-only tool catalogues for host system prompts.

/// Render a description-only catalogue and the JSON-in-tag protocol.
///
/// Empty catalogues contribute no bytes. Names and descriptions are preserved
/// verbatim, in input order, including embedded newlines. The framing and
/// protocol bytes match the legacy channel prompt so persisted prefixes remain
/// cache-compatible. Full-schema dialect catalogues belong to TinyTools.
#[must_use]
pub fn render_description_catalogue(tools: &[(&str, &str)]) -> String {
    use std::fmt::Write;
    if tools.is_empty() {
        return String::new();
    }
    let mut result = String::from("## Tools\n\nYou have access to the following tools:\n\n");
    for (name, description) in tools {
        writeln!(result, "- **{name}**: {description}").expect("writing to a String cannot fail");
    }
    result.push('\n');
    result.push_str(tinytools_agent::dialect::XmlDialect::description_protocol());
    result
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
