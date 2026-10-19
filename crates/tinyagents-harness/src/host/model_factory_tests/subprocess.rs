#[cfg(feature = "claude-code")]
use super::*;

#[cfg(feature = "claude-code")]
#[test]
fn subprocess_route_classification_accepts_existing_claude_route_spellings() {
    use tinyinference_providers::route::{ProviderRoute, RouteKind};

    assert_eq!(
        ProviderRoute::parse("claude-code:sonnet@0.2")
            .unwrap()
            .kind(),
        RouteKind::ClaudeCode
    );
    assert_eq!(
        ProviderRoute::parse("claude_agent_sdk:haiku")
            .unwrap()
            .kind(),
        RouteKind::ClaudeAgentSdk
    );
    assert!(ProviderRoute::parse("openai:gpt-4o").unwrap().kind() != RouteKind::ClaudeCode);
}

#[cfg(feature = "claude-code")]
#[test]
fn subprocess_builder_forwards_model_paths_and_mcp_without_spawning_a_process() {
    use crate::providers::claude_code::driver::McpEndpointProvider;
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
    let model = build_claude_subprocess_model_with(
        &route,
        &context,
        |model, workspace, action, passed_mcp| {
            *called.borrow_mut() = true;
            assert_eq!(model, "claude-model");
            assert_eq!(workspace, std::path::Path::new("/workspace/session"));
            assert_eq!(action, std::path::Path::new("/workspace/project"));
            assert!(Arc::ptr_eq(
                passed_mcp.as_ref().expect("MCP provider forwarded"),
                &mcp
            ));
            Ok(Arc::new(tinyinference_llm::MockModel::constant("ok")) as Arc<dyn ChatModel<()>>)
        },
    )
    .expect("subprocess route builds");
    assert!(model.is_some());
    assert!(*called.borrow());

    let cloud = ProviderRoute::parse("openai:gpt-4o").expect("route parses");
    let called = RefCell::new(false);
    assert!(
        build_claude_subprocess_model_with(&cloud, &context, |_, _, _, _| {
            *called.borrow_mut() = true;
            Ok(Arc::new(tinyinference_llm::MockModel::constant("unused"))
                as Arc<dyn ChatModel<()>>)
        })
        .expect("other providers are ignored")
        .is_none()
    );
    assert!(!*called.borrow());
}

#[cfg(feature = "claude-code")]
#[test]
fn sdk_subprocess_builder_uses_route_model_and_host_config_without_calling_cli_constructor() {
    use tinyinference_providers::route::ProviderRoute;

    let context = SubprocessProviderContext {
        workspace_dir: std::path::PathBuf::from("/workspace/session"),
        action_dir: std::path::PathBuf::from("/workspace/project"),
        claude_agent_sdk: crate::providers::claude_agent_sdk::ClaudeAgentSdkConfig {
            default_model: "host-default".to_owned(),
            ..Default::default()
        },
        mcp_provider: None,
    };
    for route in [
        "claude_agent_sdk:route-model",
        "claude-agent-sdk:second-model",
    ] {
        let route = ProviderRoute::parse(route).expect("SDK route parses");
        let model = build_claude_subprocess_model_with(&route, &context, |_, _, _, _| {
            panic!("Claude Code constructor must not handle SDK routes")
        })
        .expect("SDK route builds")
        .expect("SDK route has a model");
        assert_eq!(
            model.profile().and_then(|profile| profile.model.as_deref()),
            Some(route.model.as_str())
        );
    }
}

#[cfg(feature = "claude-code")]
#[test]
fn empty_claude_code_model_is_rejected_before_constructor_is_called() {
    use tinyinference_providers::route::ProviderRoute;

    let context = SubprocessProviderContext {
        workspace_dir: std::path::PathBuf::from("/workspace/session"),
        action_dir: std::path::PathBuf::from("/workspace/project"),
        claude_agent_sdk: crate::providers::claude_agent_sdk::ClaudeAgentSdkConfig::default(),
        mcp_provider: None,
    };
    let route = ProviderRoute::parse("claude-code:").expect("empty model remains parseable");
    let called = RefCell::new(false);
    assert!(
        build_claude_subprocess_model_with(&route, &context, |_, _, _, _| {
            *called.borrow_mut() = true;
            Ok(
                Arc::new(tinyinference_llm::MockModel::constant("unexpected"))
                    as Arc<dyn ChatModel<()>>,
            )
        })
        .is_err()
    );
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
