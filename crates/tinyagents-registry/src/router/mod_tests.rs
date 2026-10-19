//! Unit tests for [`WorkloadRouter`](super::WorkloadRouter) / [`WorkloadRoute`].

use super::*;
use std::collections::HashMap;
use tinyinference_llm::model::CapabilitySet;

/// Mirrors OpenHuman's workload tiers so the tests exercise the exact projection
/// the host router needs: fast/chat siblings, heavy reasoning siblings, and a
/// capability-gated vision primary-only tier.
fn oh_like_router() -> WorkloadRouter {
    WorkloadRouter::new()
        .with_route(WorkloadRoute::new("chat-v1", "chat-v1").with_fallbacks(["burst-v1"]))
        .with_route(WorkloadRoute::new("burst-v1", "burst-v1").with_fallbacks(["chat-v1"]))
        .with_route(
            WorkloadRoute::new("reasoning-v1", "reasoning-v1").with_fallbacks(["agentic-v1"]),
        )
        .with_route(WorkloadRoute::new("agentic-v1", "agentic-v1").with_fallbacks(["reasoning-v1"]))
        .with_route(
            WorkloadRoute::new("vision-v1", "vision-v1").requiring(CapabilitySet {
                image_in: true,
                ..CapabilitySet::default()
            }),
        )
        .with_default("chat-v1")
}

#[test]
fn projects_inference_routes_with_the_host_vision_hint() {
    let router = WorkloadRouter::from_workload_routes("hint:vision");
    assert_eq!(
        router.fallback_policy("hint:chat").unwrap().models,
        vec!["hint:chat".to_string(), "hint:burst".to_string()]
    );
    assert!(
        router
            .required_capabilities("hint:vision")
            .unwrap()
            .image_in
    );
    assert!(router.fallback_policy("hint:vision").is_none());

    let other_host = WorkloadRouter::from_workload_routes("vision-tier");
    assert!(other_host.required_capabilities("hint:vision").is_none());
}

#[test]
fn resolves_alias_to_target_model() {
    let r = oh_like_router();
    assert_eq!(r.target_model("reasoning-v1"), Some("reasoning-v1"));
    assert_eq!(r.target_model("nope"), None);
    assert_eq!(r.default_alias(), Some("chat-v1"));
}

#[test]
fn fallback_policy_heads_with_primary_then_alternates() {
    let r = oh_like_router();
    let policy = r.fallback_policy("chat-v1").expect("chat has a sibling");
    // The chain leads with the primary so `next_after(primary)` yields the alternate.
    assert_eq!(
        policy.models,
        vec!["chat-v1".to_string(), "burst-v1".to_string()]
    );
    assert_eq!(policy.next_after("chat-v1"), Some("burst-v1"));
    assert_eq!(policy.next_after("burst-v1"), None);
}

#[test]
fn fallback_policy_is_none_without_alternates() {
    let r = oh_like_router();
    // vision is primary-only (a text fallback can't satisfy image_in).
    assert!(r.fallback_policy("vision-v1").is_none());
    // Unknown alias installs no policy.
    assert!(r.fallback_policy("ghost").is_none());
}

#[test]
fn capability_gate_only_for_gated_routes() {
    let r = oh_like_router();
    let gate = r
        .required_capabilities("vision-v1")
        .expect("vision requires image_in");
    assert!(gate.image_in);
    // A plain text tier imposes no gate, so the common turn is unaffected.
    assert!(r.required_capabilities("chat-v1").is_none());
    assert!(r.required_capabilities("unknown").is_none());
}

#[test]
fn aliases_preserve_registration_order() {
    let r = oh_like_router();
    let aliases: Vec<&str> = r.aliases().collect();
    assert_eq!(
        aliases,
        vec![
            "chat-v1",
            "burst-v1",
            "reasoning-v1",
            "agentic-v1",
            "vision-v1"
        ]
    );
}

#[test]
fn register_rejects_duplicate_alias() {
    let mut r = WorkloadRouter::new();
    r.register(WorkloadRoute::new("chat-v1", "chat-v1"))
        .unwrap();
    let err = r
        .register(WorkloadRoute::new("chat-v1", "other"))
        .unwrap_err();
    assert!(err.to_string().contains("chat-v1"), "err: {err}");
    // The original mapping is intact.
    assert_eq!(r.target_model("chat-v1"), Some("chat-v1"));
}

#[test]
fn with_route_last_write_wins_and_keeps_position() {
    let r = WorkloadRouter::new()
        .with_route(WorkloadRoute::new("a", "a-model"))
        .with_route(WorkloadRoute::new("b", "b-model"))
        // Overwrite `a` in place — position preserved, target updated.
        .with_route(WorkloadRoute::new("a", "a-model-v2"));
    assert_eq!(r.target_model("a"), Some("a-model-v2"));
    assert_eq!(r.aliases().collect::<Vec<_>>(), vec!["a", "b"]);
    assert_eq!(r.routes().len(), 2);
}

#[test]
fn empty_router_answers_nothing() {
    let r = WorkloadRouter::new();
    assert!(r.is_empty());
    assert!(r.default_alias().is_none());
    assert!(r.target_model("x").is_none());
    assert!(r.fallback_policy("x").is_none());
    assert!(r.required_capabilities("x").is_none());
}

#[test]
fn workload_route_serde_skips_defaults() {
    // A bare route round-trips compactly (no `requires`/`fallbacks` noise).
    let route = WorkloadRoute::new("chat-v1", "chat-v1");
    let json = serde_json::to_string(&route).unwrap();
    assert_eq!(json, r#"{"alias":"chat-v1","model":"chat-v1"}"#);
    let back: WorkloadRoute = serde_json::from_str(&json).unwrap();
    assert_eq!(back, route);
}
#[derive(Default)]
struct TestRoutes {
    routes: HashMap<String, String>,
    primary: String,
    default: Option<String>,
    managed_default: Option<String>,
}

impl RouteSource for TestRoutes {
    fn configured_route_for_role(&self, role: &str) -> Option<&str> {
        self.routes.get(role).map(String::as_str)
    }

    fn primary_cloud_route(&self) -> &str {
        &self.primary
    }

    fn default_model(&self) -> Option<&str> {
        self.default.as_deref()
    }

    fn classify_default_model_route(&self, model: &str) -> DefaultModelRoute {
        if model.contains(':') || model == "claude_agent_sdk" {
            DefaultModelRoute::ExplicitProvider
        } else if model.starts_with("catalog/") {
            DefaultModelRoute::ManagedCatalog
        } else {
            DefaultModelRoute::Other
        }
    }

    fn is_managed_route(&self, route: &str) -> bool {
        matches!(route, "managed" | "incomplete")
    }

    fn managed_default_model(&self) -> Option<&str> {
        self.managed_default.as_deref()
    }

    fn legacy_tier_role(&self, tier: &str) -> Option<&str> {
        match tier {
            "old-chat" => Some("chat"),
            "old-memory" => Some("summarization"),
            _ => None,
        }
    }
}

fn router() -> WorkloadRouter {
    WorkloadRouter::new()
}

#[test]
fn explicit_default_routes_apply_only_to_chat_workloads() {
    let routes = TestRoutes {
        primary: "managed".into(),
        default: Some("ollama:llama3".into()),
        ..TestRoutes::default()
    };
    assert_eq!(router().provider_for_role("chat", &routes), "ollama:llama3");
    assert_eq!(
        router().provider_for_role("reasoning", &routes),
        "ollama:llama3"
    );
    assert_eq!(router().provider_for_role("vision", &routes), "managed");
}

#[test]
fn managed_catalog_and_unknown_defaults_do_not_change_provider_route() {
    for default in ["catalog/model-a", "unregistered-model"] {
        let routes = TestRoutes {
            primary: "managed".into(),
            default: Some(default.into()),
            ..TestRoutes::default()
        };
        assert_eq!(router().provider_for_role("chat", &routes), "managed");
    }
}

#[test]
fn host_classifies_local_and_claude_defaults_as_explicit_routes() {
    for default in ["ollama:llama3", "claude_agent_sdk"] {
        let routes = TestRoutes {
            primary: "managed".into(),
            default: Some(default.into()),
            ..TestRoutes::default()
        };
        assert_eq!(router().provider_for_role("chat", &routes), default);
    }
}

#[test]
fn configured_role_is_independent_and_cloud_fallback_metadata_is_typed() {
    let routes = TestRoutes {
        routes: HashMap::from([("coding".into(), "byok:model".into())]),
        primary: "managed".into(),
        ..TestRoutes::default()
    };
    let router = router();
    assert_eq!(router.provider_for_role("coding", &routes), "byok:model");
    assert_eq!(router.provider_for_role("chat", &routes), "managed");

    let diag = router
        .fallback_diagnostics("summarization", &routes)
        .unwrap();
    assert_eq!(diag.role, "summarization");
    assert_eq!(diag.requested_route, None);
    assert_eq!(diag.fallback_route, "managed");
    assert_eq!(diag.reason, FallbackReason::ImplicitCloudFallback);
    assert_eq!(diag.override_role, "memory");
    assert!(router.fallback_diagnostics("chat", &routes).is_none());
    assert_eq!(
        router.cloud_fallback_role("burst").unwrap().override_role,
        "agentic"
    );
    assert_eq!(
        router
            .cloud_fallback_role("summarization")
            .unwrap()
            .override_role,
        "memory"
    );
    assert!(router.cloud_fallback_role("chat").is_none());
}

#[test]
fn model_helpers_preserve_hints_legacy_aliases_and_raw_models() {
    let routes = TestRoutes {
        primary: "managed".into(),
        managed_default: Some("catalog/default".into()),
        ..TestRoutes::default()
    };
    let router = router();
    assert_eq!(
        router.role_for_model_tier("hint:reasoning", &routes),
        "reasoning"
    );
    assert_eq!(
        router.role_for_model_tier("old-memory", &routes),
        "summarization"
    );
    assert_eq!(router.role_for_model_tier("mystery", &routes), "chat");
    assert_eq!(
        router.resolve_model_for_hint(
            "hint:chat",
            &TestRoutes {
                primary: "managed".into(),
                ..TestRoutes::default()
            }
        ),
        None,
        "missing managed model is represented as unavailable"
    );
    assert_eq!(
        router.resolve_model_for_hint("hint:reasoning", &routes),
        Some("catalog/default".into())
    );
    assert_eq!(
        router.resolve_model_for_hint(
            "hint:chat",
            &TestRoutes {
                routes: HashMap::from([("chat".into(), "vendor:model@0.4".into())]),
                ..TestRoutes::default()
            }
        ),
        Some("model".into())
    );
    assert!(router.is_raw_passthrough_model("openrouter/vendor/model", &routes));
    assert!(!router.is_raw_passthrough_model("old-chat", &routes));
    assert!(!router.is_raw_passthrough_model("hint:unknown", &routes));
}
