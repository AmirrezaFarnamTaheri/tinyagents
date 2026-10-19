use super::*;

use std::cell::RefCell;

use crate::error::TinyAgentsError;
use crate::host::model_resolver::{ModelResolveRequest, ModelResolver};
use tinyinference_llm::model::ModelRequest;

#[test]
fn primary_override_stays_separate_from_default_workload_routes_and_summary() {
    let context = NonCloneContext {
        primary_provider_override: Some("provider-override".to_owned()),
    };
    let factory = RecordingFactory::new(&context);
    let gate = RecordingGate::default();
    let opts = BuildOpts {
        native_tools: true,
        temperature: Some(0.25),
        egress_tag: Some("turn".to_owned()),
    };

    let models = TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .primary_model_pin("primary-model")
        .primary_opts(opts.clone())
        .build()
        .expect("turn models should build");

    let requests = factory.requests.borrow();
    assert_eq!(requests[0].role, "chat");
    assert_eq!(requests[0].purpose, ModelPurpose::Primary);
    assert_eq!(requests[0].model_pin.as_deref(), Some("primary-model"));
    assert_eq!(requests[0].opts, opts);
    assert_eq!(requests[1].purpose, ModelPurpose::WorkloadTier("hint:chat".into()));
    assert_eq!(requests[1].role, "chat");
    assert_eq!(requests[1].route.as_deref(), Some("hint:chat"));
    assert_eq!(requests[1].model_pin.as_deref(), Some("hint:chat"));
    let default_tiers = requests
        .iter()
        .filter_map(|request| match &request.purpose {
            ModelPurpose::WorkloadTier(route) => Some((route.as_str(), request.role.as_str())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        default_tiers,
        [
            ("hint:chat", "chat"),
            ("hint:reasoning", "reasoning"),
            ("hint:agentic", "agentic"),
            ("hint:coding", "coding"),
            ("hint:burst", "burst"),
            ("hint:summarization", "summarization"),
            ("hint:vision", "vision"),
        ]
    );
    assert_eq!(requests.last().expect("summary").role, "chat");
    assert_eq!(requests.last().expect("summary").purpose, ModelPurpose::Summarization);
    assert_eq!(requests.last().expect("summary").model_pin.as_deref(), Some("primary-model"));
    assert_eq!(requests.last().expect("summary").opts, opts);
    assert_eq!(models.primary_route().provider, "provider-override");
    assert_eq!(models.summarizer_route().provider, "provider-override");
    assert_eq!(context.primary_provider_override.as_deref(), Some("provider-override"));
    assert_eq!(gate.requests.borrow().as_slice(), requests.as_slice());
}

#[test]
fn explicit_summary_role_pin_and_options_are_distinct() {
    let context = NonCloneContext {
        primary_provider_override: Some("primary-provider".to_owned()),
    };
    let factory = RecordingFactory::new(&context);
    let gate = RecordingGate::default();
    let summary_opts = BuildOpts {
        native_tools: false,
        temperature: Some(0.0),
        egress_tag: Some("summary".to_owned()),
    };
    let models = TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .without_workload_tiers()
        .primary_model_pin("chat-model")
        .summarization("summarization", Some("summary-model".to_owned()))
        .summarization_opts(summary_opts.clone())
        .build()
        .expect("summary model should build");

    let requests = factory.requests.borrow();
    let summary = requests.last().expect("summary request");
    assert_eq!(summary.role, "summarization");
    assert_eq!(summary.model_pin.as_deref(), Some("summary-model"));
    assert_eq!(summary.opts, summary_opts);
    assert_eq!(models.summarizer_route().model, "summary-model");
}

#[test]
fn custom_workload_requests_preserve_order_and_skip_failed_or_gated_routes() {
    let context = NonCloneContext {
        primary_provider_override: None,
    };
    let factory = RecordingFactory::new(&context).failing_route(Some("hint:broken"));
    let gate = RecordingGate::default().denying_route(Some("hint:blocked"));
    let requests = vec![
        workload("planner", "hint:first", None),
        workload("coder", "hint:blocked", None),
        workload("fallback", "hint:broken", None),
        workload("scribe", "hint:last", None),
    ];

    let models = TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .workload_requests(requests)
        .build()
        .expect("failed tiers should not fail the set");

    let built_routes = factory
        .requests
        .borrow()
        .iter()
        .filter_map(|request| request.route.clone())
        .collect::<Vec<_>>();
    assert_eq!(built_routes, ["hint:first", "hint:broken", "hint:last"]);
    assert!(models.workload_error_slot("hint:first").is_some());
    assert!(models.workload_error_slot("hint:blocked").is_none());
    assert!(models.workload_error_slot("hint:broken").is_none());
    assert!(models.workload_error_slot("hint:last").is_some());
}

#[test]
fn denied_primary_never_calls_factory_and_failed_primary_is_fatal() {
    let context = NonCloneContext {
        primary_provider_override: None,
    };
    let factory = RecordingFactory::new(&context);
    let gate = RecordingGate::default().denying_role(Some("chat"));
    assert!(TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .build()
        .is_err());
    assert!(factory.requests.borrow().is_empty());

    let factory = RecordingFactory::new(&context).failing_primary();
    let gate = RecordingGate::default();
    assert!(TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .build()
        .is_err());
}

#[test]
fn denied_or_failed_summary_is_fatal_after_primary_build() {
    let context = NonCloneContext {
        primary_provider_override: None,
    };
    let gate = RecordingGate::default().denying_purpose(Some(ModelPurpose::Summarization));
    let factory = RecordingFactory::new(&context);
    assert!(TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .without_workload_tiers()
        .build()
        .is_err());
    assert_eq!(factory.requests.borrow().len(), 1);

    let factory = RecordingFactory::new(&context).failing_summary();
    let gate = RecordingGate::default();
    assert!(TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .without_workload_tiers()
        .build()
        .is_err());
}

#[test]
fn different_workload_route_with_same_concrete_primary_pin_is_kept() {
    let context = NonCloneContext {
        primary_provider_override: None,
    };
    let factory = RecordingFactory::new(&context);
    let gate = RecordingGate::default();

    TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .primary_model_pin("same-model")
        .workload_requests(vec![
            workload("other", "hint:other", Some("other-model")),
            workload("same", "hint:same", Some("same-model")),
        ])
        .build()
        .expect("turn models should build");

    let routes = factory
        .requests
        .borrow()
        .iter()
        .filter_map(|request| request.route.clone())
        .collect::<Vec<_>>();
    assert_eq!(routes, ["hint:other", "hint:same"]);
}

#[test]
fn shared_workload_options_forward_to_every_default_tier_with_route_override() {
    let context = NonCloneContext {
        primary_provider_override: None,
    };
    let factory = RecordingFactory::new(&context);
    let gate = RecordingGate::default();
    let common = BuildOpts {
        native_tools: true,
        temperature: Some(0.4),
        egress_tag: Some("workload".to_owned()),
    };
    let override_opts = BuildOpts {
        native_tools: false,
        temperature: Some(0.0),
        egress_tag: Some("vision".to_owned()),
    };

    TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .workload_opts(common.clone())
        .workload_tier_opts("hint:vision", override_opts.clone())
        .build()
        .expect("turn models should build");

    let tier_opts = factory
        .requests
        .borrow()
        .iter()
        .filter(|request| matches!(request.purpose, ModelPurpose::WorkloadTier(_)))
        .map(|request| (request.route.as_deref().unwrap(), request.opts.clone()))
        .collect::<Vec<_>>();
    assert_eq!(tier_opts.len(), 7);
    for (route, opts) in tier_opts {
        if route == "hint:vision" {
            assert_eq!(opts, override_opts);
        } else {
            assert_eq!(opts, common);
        }
    }
}

#[test]
fn default_workload_matching_primary_alias_is_skipped_but_other_alias_with_same_pin_is_kept() {
    let context = NonCloneContext {
        primary_provider_override: None,
    };
    let factory = RecordingFactory::new(&context);
    let gate = RecordingGate::default();
    TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .primary_model_pin("hint:chat")
        .build()
        .expect("models should build");
    assert!(!factory.requests.borrow().iter().any(|request| {
        request.purpose == ModelPurpose::WorkloadTier("hint:chat".into())
    }));

    let factory = RecordingFactory::new(&context);
    TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .primary_model_pin("shared-model")
        .workload_requests(vec![workload(
            "alternate",
            "hint:alternate",
            Some("shared-model"),
        )])
        .build()
        .expect("models should build");
    assert!(factory.requests.borrow().iter().any(|request| {
        request.route.as_deref() == Some("hint:alternate")
    }));
}

#[tokio::test]
async fn resolver_keeps_lead_pin_hint_and_fallback_precedence() {
    let context = NonCloneContext {
        primary_provider_override: None,
    };
    let factory = RecordingFactory::new(&context);
    let gate = RecordingGate::default();
    let models = TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .without_workload_tiers()
        .workload_requests(vec![
            workload("pinned-role", "pinned-model", Some("pinned-model")),
            workload("hint-role", "hint:fast", None),
        ])
        .build()
        .expect("models should build");

    assert_eq!(invoke_text(&models, ModelResolveRequest::new("lead").as_team_lead()).await, "chat");
    assert_eq!(invoke_text(&models, ModelResolveRequest::new("pinned").with_model_pin("pinned-model")).await, "pinned-model");
    assert_eq!(invoke_text(&models, ModelResolveRequest::new("hinted").with_model_hint("fast")).await, "hint:fast");
    assert_eq!(invoke_text(&models, ModelResolveRequest::new("fallback")).await, "chat");
}

#[tokio::test]
async fn summarizer_uses_a_distinct_model_and_error_slot() {
    let context = NonCloneContext {
        primary_provider_override: None,
    };
    let factory = RecordingFactory::new(&context);
    let gate = RecordingGate::default();
    let models = TurnModelSet::<()>::builder(&factory, &gate, "chat")
        .without_workload_tiers()
        .build()
        .expect("models should build");

    let lead = models
        .resolver()
        .resolve(&ModelResolveRequest::new("lead").as_team_lead())
        .await
        .expect("primary resolver model");
    let summary = models.summarizer();
    assert!(!Arc::ptr_eq(&lead, &summary));
    assert!(!Arc::ptr_eq(
        &models.primary_error_slot(),
        &models.summarizer_error_slot()
    ));
}

#[cfg(feature = "claude-code")]
#[test]
fn subprocess_route_classification_accepts_existing_claude_route_spellings() {
    use tinyinference_providers::route::{ProviderRoute, RouteKind};

    assert_eq!(ProviderRoute::parse("claude-code:sonnet@0.2").unwrap().kind(), RouteKind::ClaudeCode);
    assert_eq!(ProviderRoute::parse("claude_agent_sdk:haiku").unwrap().kind(), RouteKind::ClaudeAgentSdk);
    assert!(ProviderRoute::parse("openai:gpt-4o").unwrap().kind() != RouteKind::ClaudeCode);
}

#[cfg(feature = "claude-code")]
#[test]
fn subprocess_builder_forwards_model_paths_and_mcp_without_spawning_a_process() {
    use crate::providers::claude_code::driver::{McpEndpoint, McpEndpointProvider};
    use tinyinference_providers::route::ProviderRoute;

    let mcp: Arc<dyn McpEndpointProvider> = Arc::new(TestMcpEndpoint);
    let context = SubprocessProviderContext {
        workspace_dir: std::path::PathBuf::from("/workspace/session"),
        action_dir: std::path::PathBuf::from("/workspace/project"),
        claude_agent_sdk: crate::providers::claude_agent_sdk::ClaudeAgentSdkConfig::default(),
        mcp_provider: Some(Arc::clone(&mcp)),
    };
    let route = ProviderRoute::parse("claude-code:claude-model@0.4").expect("route parses");
    let called = RefCell::new(false);
    let model = build_claude_subprocess_model_with(&route, &context, |model, workspace, action, passed_mcp| {
        *called.borrow_mut() = true;
        assert_eq!(model, "claude-model");
        assert_eq!(workspace, std::path::Path::new("/workspace/session"));
        assert_eq!(action, std::path::Path::new("/workspace/project"));
        assert!(Arc::ptr_eq(passed_mcp.as_ref().expect("MCP provider forwarded"), &mcp));
        Ok(Arc::new(tinyinference_llm::MockModel::constant("ok")) as Arc<dyn ChatModel<()>>)
    })
    .expect("subprocess route builds");
    assert!(model.is_some());
    assert!(*called.borrow());

    let cloud = ProviderRoute::parse("openai:gpt-4o").expect("route parses");
    let called = RefCell::new(false);
    assert!(build_claude_subprocess_model_with(&cloud, &context, |_, _, _, _| {
        *called.borrow_mut() = true;
        Ok(Arc::new(tinyinference_llm::MockModel::constant("unused")) as Arc<dyn ChatModel<()>>)
    })
    .expect("other providers are ignored")
    .is_none());
    assert!(!*called.borrow());
}

#[cfg(feature = "claude-code")]
struct TestMcpEndpoint;

#[cfg(feature = "claude-code")]
#[async_trait::async_trait]
impl crate::providers::claude_code::driver::McpEndpointProvider for TestMcpEndpoint {
    async fn endpoint(
        &self,
    ) -> std::result::Result<crate::providers::claude_code::driver::McpEndpoint, String> {
        Err("not called by the builder test".to_owned())
    }
}

async fn invoke_text(
    models: &TurnModelSet<()>,
    request: ModelResolveRequest,
) -> String {
    models
        .resolver()
        .resolve(&request)
        .await
        .expect("route resolves")
        .invoke(&(), ModelRequest::default())
        .await
        .expect("mock responds")
        .text()
        .to_owned()
}

fn workload(role: &str, route: &str, model_pin: Option<&str>) -> ModelBuildRequest {
    ModelBuildRequest {
        role: role.to_owned(),
        route: Some(route.to_owned()),
        purpose: ModelPurpose::WorkloadTier(route.to_owned()),
        model_pin: model_pin.map(str::to_owned),
        opts: BuildOpts::default(),
    }
}

#[derive(Default)]
struct NonCloneContext {
    primary_provider_override: Option<String>,
}

struct RecordingFactory<'a> {
    context: &'a NonCloneContext,
    requests: RefCell<Vec<ModelBuildRequest>>,
    failed_route: Option<String>,
    fail_primary: bool,
    fail_summary: bool,
}

impl<'a> RecordingFactory<'a> {
    fn new(context: &'a NonCloneContext) -> Self {
        Self {
            context,
            requests: RefCell::new(Vec::new()),
            failed_route: None,
            fail_primary: false,
            fail_summary: false,
        }
    }

    fn failing_route(mut self, route: Option<&str>) -> Self {
        self.failed_route = route.map(str::to_owned);
        self
    }

    fn failing_primary(mut self) -> Self {
        self.fail_primary = true;
        self
    }

    fn failing_summary(mut self) -> Self {
        self.fail_summary = true;
        self
    }
}

impl ModelFactory<()> for RecordingFactory<'_> {
    fn build(
        &self,
        request: &ModelBuildRequest,
    ) -> Result<(Arc<dyn ChatModel<()>>, ResolvedModelRoute)> {
        self.requests.borrow_mut().push(request.clone());
        if (self.fail_primary && request.purpose == ModelPurpose::Primary)
            || (self.fail_summary && request.purpose == ModelPurpose::Summarization)
            || self
                .failed_route
                .as_deref()
                .is_some_and(|route| request.route.as_deref() == Some(route))
        {
            return Err(TinyAgentsError::Model("fixture failure".into()));
        }
        let provider = if matches!(
            request.purpose,
            ModelPurpose::Primary | ModelPurpose::Summarization
        ) {
            self.context
                .primary_provider_override
                .as_deref()
                .unwrap_or("factory")
        } else {
            "factory"
        };
        let model_id = request
            .model_pin
            .as_deref()
            .or(request.route.as_deref())
            .unwrap_or(&request.role);
        Ok((
            Arc::new(tinyinference_llm::MockModel::constant(model_id)) as Arc<dyn ChatModel<()>>,
            ResolvedModelRoute::new(provider, model_id, request.route.as_deref().unwrap_or(&request.role)),
        ))
    }
}

#[derive(Default)]
struct RecordingGate {
    requests: RefCell<Vec<ModelBuildRequest>>,
    denied_role: Option<String>,
    denied_route: Option<String>,
    denied_purpose: Option<ModelPurpose>,
}

impl RecordingGate {
    fn denying_role(mut self, role: Option<&str>) -> Self {
        self.denied_role = role.map(str::to_owned);
        self
    }

    fn denying_route(mut self, route: Option<&str>) -> Self {
        self.denied_route = route.map(str::to_owned);
        self
    }

    fn denying_purpose(mut self, purpose: Option<ModelPurpose>) -> Self {
        self.denied_purpose = purpose;
        self
    }
}

impl ModelBuildGate for RecordingGate {
    fn check(&self, request: &ModelBuildRequest) -> Result<()> {
        self.requests.borrow_mut().push(request.clone());
        if self.denied_role.as_deref() == Some(&request.role)
            || self
                .denied_route
                .as_deref()
                .is_some_and(|route| request.route.as_deref() == Some(route))
            || self.denied_purpose.as_ref() == Some(&request.purpose)
        {
            return Err(TinyAgentsError::Model("blocked by fixture gate".into()));
        }
        Ok(())
    }
}
