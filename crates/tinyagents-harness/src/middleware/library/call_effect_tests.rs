use super::*;

#[test]
fn the_effect_of_a_dispatcher_is_the_effect_of_its_target() {
    let list = serde_json::json!({"skill": "composio", "tool": "composio_list_tools", "args": {}});
    assert_eq!(call_effect(None, "use_skill", &list), CallEffect::ReadOnly);
    let send = serde_json::json!({"skill": "gmail", "tool": "gmail_send_email", "args": {}});
    assert_eq!(
        call_effect(None, "use_skill", &send),
        CallEffect::SideEffecting
    );
    let fetch = serde_json::json!({"tool": "GMAIL_FETCH_EMAILS", "arguments": {}});
    assert_eq!(
        call_effect(None, "composio_execute", &fetch),
        CallEffect::ReadOnly
    );
    let target = dispatch_target("use_skill", &list).expect("use_skill names a target");
    assert_eq!(target.skill, Some("composio"));
    assert_eq!(target.tool, "composio_list_tools");
}

#[test]
fn declared_metadata_outranks_the_tool_name() {
    let lookup: ToolFactsLookup = Arc::new(|name: &str, _args: &Value| match name {
        "frobnicate" => Some(ToolEffectFacts {
            classified: Some(CallEffect::ReadOnly),
            external: false,
            elevated: false,
        }),
        "list_and_notify" => Some(ToolEffectFacts {
            classified: None,
            external: true,
            elevated: false,
        }),
        _ => None,
    });
    let args = serde_json::json!({});
    assert_eq!(
        call_effect(Some(&lookup), "frobnicate", &args),
        CallEffect::ReadOnly
    );
    assert_eq!(
        call_effect(Some(&lookup), "list_and_notify", &args),
        CallEffect::SideEffecting
    );
}

#[test]
fn a_mutating_verb_wins_over_a_reading_one() {
    assert_eq!(name_effect("list_and_send"), CallEffect::SideEffecting);
    assert_eq!(name_effect("composio_list_tools"), CallEffect::ReadOnly);
    assert_eq!(name_effect("frobnicate"), CallEffect::Unknown);
}

#[test]
fn an_unknown_name_without_elevated_permission_is_read_only() {
    let lookup: ToolFactsLookup = Arc::new(|_: &str, _: &Value| {
        Some(ToolEffectFacts {
            classified: None,
            external: false,
            elevated: false,
        })
    });
    assert_eq!(
        call_effect(Some(&lookup), "frobnicate", &serde_json::json!({})),
        CallEffect::ReadOnly
    );
    assert_eq!(
        call_effect(None, "frobnicate", &serde_json::json!({})),
        CallEffect::Unknown
    );
}
