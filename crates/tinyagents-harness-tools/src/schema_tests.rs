use super::*;

#[test]
fn embedded_schema_parses_and_returns_independent_values() {
    let mut first = crate::static_schema!(r#"{"type":"object","required":["command"]}"#);
    let second = crate::static_schema!(r#"{"type":"object","required":["command"]}"#);
    first["type"] = "changed".into();
    assert_eq!(second["type"], "object");
    assert_eq!(second["required"][0], "command");
}

#[test]
#[should_panic(expected = "embedded tool schema at test-site is not valid JSON")]
fn malformed_embedded_schema_names_the_call_site() {
    let _ = parse_embedded("test-site", "not json");
}
