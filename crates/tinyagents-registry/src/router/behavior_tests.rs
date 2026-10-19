use super::*;
use std::collections::HashMap;

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
        router.cloud_fallback_role("summarization").unwrap().override_role,
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
    assert_eq!(router.role_for_model_tier("hint:reasoning", &routes), "reasoning");
    assert_eq!(router.role_for_model_tier("old-memory", &routes), "summarization");
    assert_eq!(router.role_for_model_tier("mystery", &routes), "chat");
    assert_eq!(
        router.resolve_model_for_hint("hint:chat", &TestRoutes {
            primary: "managed".into(),
            ..TestRoutes::default()
        }),
        None,
        "missing managed model is represented as unavailable"
    );
    assert_eq!(
        router.resolve_model_for_hint("hint:reasoning", &routes),
        Some("catalog/default".into())
    );
    assert_eq!(router.resolve_model_for_hint("hint:chat", &TestRoutes {
        routes: HashMap::from([("chat".into(), "vendor:model@0.4".into())]),
        ..TestRoutes::default()
    }), Some("model".into()));
    assert!(router.is_raw_passthrough_model("openrouter/vendor/model", &routes));
    assert!(!router.is_raw_passthrough_model("old-chat", &routes));
    assert!(!router.is_raw_passthrough_model("hint:unknown", &routes));
}
