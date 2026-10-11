//! Unit tests for the run's tool gate: how the allowlist, the harness policy
//! and a definition's rules combine.

use super::*;

use std::collections::HashSet;

use serde_json::json;
use tinytools::{ApprovalDirective, RuleContext, Surface, Tool, ToolRules};

fn deny(pattern: &str) -> ToolRules {
    ToolRules::from_allow_deny(Vec::<String>::new(), [pattern])
}

#[test]
fn a_default_gate_admits_everything() {
    let gate = ToolGate::default();
    assert!(gate.allows_name("x"));
    assert!(gate.lists("x", None, Surface::Catalog));
    assert!(ToolRulePolicy::default().is_permissive());
}

#[test]
fn the_allowlist_still_applies_first() {
    let allowed: HashSet<String> = ["a".to_string()].into();
    let gate = ToolGate::new(Some(allowed), &ToolRulePolicy::default(), None);
    assert!(gate.lists("a", None, Surface::Search));
    assert!(!gate.lists("b", None, Surface::Search));
    assert!(!gate.allows_name("b"));
}

#[test]
fn policy_and_definition_rules_both_apply() {
    let policy = ToolRulePolicy::new(deny("x_*"));
    let definition = deny("y_*");
    let gate = ToolGate::new(None, &policy, Some(&definition));
    assert!(!gate.lists("x_1", None, Surface::Catalog));
    assert!(!gate.lists("y_1", None, Surface::Catalog));
    assert!(gate.lists("z_1", None, Surface::Catalog));
}

#[test]
fn a_permissive_definition_layer_is_ignored() {
    let policy = ToolRulePolicy::new(deny("x_*"));
    let gate = ToolGate::new(None, &policy, Some(&ToolRules::allow_all()));
    assert!(!gate.lists("x_1", None, Surface::Catalog));
    let empty = ToolGate::new(
        None,
        &ToolRulePolicy::default(),
        Some(&ToolRules::allow_all()),
    );
    assert!(empty.lists("anything", None, Surface::Catalog));
}

#[test]
fn the_policy_context_reaches_when_conditions() {
    let rules: ToolRules = serde_json::from_value(json!({ "rules": [
        { "effect": "deny", "match": { "name": "shell" }, "when": { "channel": "web" } },
    ] }))
    .expect("rules");
    let web =
        ToolRulePolicy::new(rules.clone()).with_context(RuleContext::new().with("channel", "web"));
    let cli = ToolRulePolicy::new(rules).with_context(RuleContext::new().with("channel", "cli"));
    assert!(!ToolGate::new(None, &web, None).lists("shell", None, Surface::Catalog));
    assert!(ToolGate::new(None, &cli, None).lists("shell", None, Surface::Catalog));
}

struct Named(&'static str, tinytools::ToolExposure);

#[async_trait::async_trait]
impl Tool for Named {
    fn name(&self) -> &str {
        self.0
    }
    fn description(&self) -> &str {
        "named"
    }
    fn parameters_schema(&self) -> serde_json::Value {
        json!({"type": "object"})
    }
    fn exposure(&self) -> tinytools::ToolExposure {
        self.1
    }
    async fn execute(&self, _args: serde_json::Value) -> anyhow::Result<tinytools::ToolResult> {
        Ok(tinytools::ToolResult::success("ok"))
    }
}

#[test]
fn lists_tool_picks_the_surface_from_exposure() {
    let rules: ToolRules = serde_json::from_value(json!({ "rules": [
        { "effect": "deny", "on": ["search"], "match": { "name": "*" } },
    ] }))
    .expect("rules");
    let gate = ToolGate::new(None, &ToolRulePolicy::new(rules), None);
    assert!(gate.lists_tool(&Named("direct", tinytools::ToolExposure::Direct)));
    assert!(!gate.lists_tool(&Named("deferred", tinytools::ToolExposure::Deferred)));
}

#[tokio::test]
async fn admit_call_reports_approval_and_refusals() {
    let rules: ToolRules = serde_json::from_value(json!({ "rules": [
        { "effect": "require_approval", "match": { "name": "send" } },
        { "id": "nope", "effect": "deny", "match": { "name": "drop" } },
    ] }))
    .expect("rules");
    let gate = ToolGate::new(None, &ToolRulePolicy::new(rules), None);
    let direct = tinytools::ToolExposure::Direct;
    assert_eq!(
        gate.admit_call_async(&Named("send", direct), &json!({})).await,
        CallGate::Admit(ApprovalDirective::Required)
    );
    assert_eq!(
        gate.admit_call_async(&Named("read", direct), &json!({})).await,
        CallGate::Admit(ApprovalDirective::Default)
    );
    match gate.admit_call_async(&Named("drop", direct), &json!({})).await {
        CallGate::Refuse(message) => assert!(message.contains("rule 'nope'"), "{message}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(
        ToolGate::default()
            .admit_call_async(&Named("drop", direct), &json!({}))
            .await,
        CallGate::Admit(ApprovalDirective::Default)
    );
}

struct AsyncTargetFailure;

#[async_trait::async_trait]
impl Tool for AsyncTargetFailure {
    fn name(&self) -> &'static str {
        "mcp_call_tool"
    }

    fn description(&self) -> &'static str {
        "Calls a module-owned tool"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({ "type": "object" })
    }

    async fn execute(&self, _args: serde_json::Value) -> anyhow::Result<tinytools::ToolResult> {
        Ok(tinytools::ToolResult::success("ok"))
    }

    async fn indirect_target_async(
        &self,
        _args: &serde_json::Value,
    ) -> anyhow::Result<Option<tinytools::IndirectCall>> {
        anyhow::bail!("sensitive module diagnostic")
    }
}

#[tokio::test]
async fn async_target_resolution_failure_refuses_without_exposing_diagnostic() {
    let gate = ToolGate::new(
        None,
        &ToolRulePolicy::new(deny("mcp_*_delete_*")),
        None,
    );

    match gate
        .admit_call_async(&AsyncTargetFailure, &json!({}))
        .await
    {
        CallGate::Refuse(message) => {
            assert!(!message.contains("sensitive module diagnostic"));
        }
        CallGate::Admit(_) => panic!("a target resolution error must fail closed"),
    }
}

#[test]
fn an_intrinsic_is_admitted_without_rules_and_ignores_the_allowlist() {
    let allowed: HashSet<String> = ["registered".to_string()].into();
    let gate = ToolGate::new(Some(allowed), &ToolRulePolicy::default(), None);
    assert_eq!(
        gate.admits_intrinsic("tool_search", Surface::Call),
        CallGate::Admit(ApprovalDirective::Default),
        "the registration allowlist does not name intrinsics"
    );
}

#[test]
fn an_intrinsic_follows_the_rules_per_surface() {
    let rules: ToolRules = serde_json::from_value(json!({ "rules": [
        { "id": "quiet", "effect": "hide", "match": { "name": "tool_search" } },
        { "effect": "require_approval", "match": { "name": "tool_search" } },
    ] }))
    .expect("rules");
    let gate = ToolGate::new(None, &ToolRulePolicy::new(rules), None);
    match gate.admits_intrinsic("tool_search", Surface::Catalog) {
        CallGate::Refuse(message) => assert!(message.contains("rule 'quiet'"), "{message}"),
        other => panic!("hidden from the catalogue, got {other:?}"),
    }
    assert_eq!(
        gate.admits_intrinsic("tool_search", Surface::Call),
        CallGate::Admit(ApprovalDirective::Required)
    );
    let deny: ToolRules = serde_json::from_value(json!({ "rules": [
        { "effect": "deny", "match": { "name": "tool_search" } },
    ] }))
    .expect("rules");
    let gate = ToolGate::new(None, &ToolRulePolicy::new(deny), None);
    assert!(matches!(
        gate.admits_intrinsic("tool_search", Surface::Call),
        CallGate::Refuse(_)
    ));
}

#[test]
fn exact_denials_apply_to_intrinsic_tools() {
    let gate = ToolGate::new(None, &ToolRulePolicy::default(), None)
        .with_denied_names(["tool_search".to_string()]);

    assert!(matches!(
        gate.admits_intrinsic("tool_search", Surface::Catalog),
        CallGate::Refuse(_)
    ));
    assert!(matches!(
        gate.admits_intrinsic("tool_search", Surface::Call),
        CallGate::Refuse(_)
    ));
}

#[test]
fn exact_exclusions_apply_to_dynamic_wildcards_without_interpreting_patterns() {
    let gate = ToolGate::new(None, &ToolRulePolicy::default(), None)
        .with_denied_names(["blocked".to_string(), "literal*".to_string()]);
    assert!(!gate.allows_name("blocked"));
    assert!(!gate.allows_name("literal*"));
    assert!(gate.allows_name("literal_suffix"));
    assert!(gate.lists("live_tool", None, Surface::Catalog));
    let restricted = ToolGate::new(
        Some(["registered".to_string()].into()),
        &ToolRulePolicy::default(),
        None,
    )
    .with_denied_names(["registered".to_string()]);
    assert!(!restricted.allows_name("registered"));
    assert!(!restricted.allows_name("live_tool"));
}

#[test]
fn definition_deferral_is_local_and_never_promotes_hidden_registrations() {
    let direct = Named("selected", tinytools::ToolExposure::Direct);
    let hidden = Named("hidden", tinytools::ToolExposure::Hidden);
    let gate =
        ToolGate::default().with_deferred_names(["selected".to_string(), "hidden".to_string()]);
    assert_eq!(gate.exposure(&direct), tinytools::ToolExposure::Deferred);
    assert_eq!(gate.exposure(&hidden), tinytools::ToolExposure::Hidden);
    assert_eq!(direct.exposure(), tinytools::ToolExposure::Direct);
    assert_eq!(
        ToolGate::default().exposure(&direct),
        tinytools::ToolExposure::Direct
    );
}
