use super::*;

#[test]
fn local_and_user_agent_scopes_keep_the_storage_namespace() {
    let local = local_scope();
    assert_eq!(agent_scope(&local, "planner").unwrap().as_str(), "local/agent:planner");
    let user = user_scope("alice").unwrap();
    assert_eq!(user.as_str(), "u:alice");
    assert_eq!(agent_scope(&user, "planner").unwrap().as_str(), "u:alice/agent:planner");
}

#[test]
fn unsafe_scope_segments_are_hashed_and_invalid_tenants_refused() {
    let local = local_scope();
    let long = agent_scope(&local, &"x".repeat(97)).unwrap();
    assert!(long.as_str().starts_with("local/agent:~"));
    assert_eq!(agent_scope(&local, "bad id").unwrap().as_str().len(), "local/agent:".len() + 27);
    let principal = Scope::new("local/agent:other").unwrap();
    assert_eq!(agent_scope(&principal, "planner").unwrap_err().kind(), tinystoragedrivers_types::ErrorKind::InvalidInput);
    assert_eq!(user_scope("").unwrap_err().kind(), tinystoragedrivers_types::ErrorKind::InvalidInput);
}
