use super::*;

#[test]
fn session_status_roundtrip() {
    for status in [
        SessionStatus::Running,
        SessionStatus::Completed,
        SessionStatus::Failed,
        SessionStatus::Interrupted,
    ] {
        assert_eq!(SessionStatus::parse(status.as_str()), status);
    }
}

#[test]
fn session_status_parse_unknown_defaults_to_running() {
    assert_eq!(SessionStatus::parse("bogus"), SessionStatus::Running);
    assert_eq!(SessionStatus::parse(""), SessionStatus::Running);
}

#[test]
fn session_status_serde_roundtrip() {
    let status = SessionStatus::Completed;
    let json = serde_json::to_string(&status).unwrap();
    assert_eq!(json, "\"completed\"");
    let parsed: SessionStatus = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, status);
}

#[test]
fn session_search_params_defaults() {
    let params = SessionSearchParams::default();
    assert!(params.query.is_none());
    assert!(params.agent_id.is_none());
    assert!(params.limit.is_none());
    assert!(params.offset.is_none());
}

#[test]
fn search_params_deserialize_from_camel_case_with_every_field_optional() {
    let params: SessionSearchParams = serde_json::from_value(serde_json::json!({
        "query": "parser",
        "agentId": "coder",
        "toolName": "grep",
        "sourceChannel": "web",
        "parentSessionId": "p",
        "threadId": "t",
        "limit": 5,
        "offset": 2
    }))
    .unwrap();
    assert_eq!(params.agent_id.as_deref(), Some("coder"));
    assert_eq!((params.limit, params.offset), (Some(5), Some(2)));
    let empty: SessionSearchParams = serde_json::from_value(serde_json::json!({})).unwrap();
    assert!(empty.query.is_none());
}
