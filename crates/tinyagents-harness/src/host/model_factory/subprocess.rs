use std::sync::Arc;

use tinyinference_llm::model::ChatModel;

/// Host-owned context passed to the Claude subprocess provider constructors.
#[cfg(feature = "claude-code")]
pub struct SubprocessProviderContext {
    /// Working directory used by Claude Code.
    pub workspace_dir: std::path::PathBuf,
    /// Action/project directory used by Claude Code.
    pub action_dir: std::path::PathBuf,
    /// Host-owned document port for provider session/settings state.
    pub documents: Arc<dyn tinyagents_storage_kit::DocumentStore>,
    /// Host-owned Claude Agent SDK settings.
    pub claude_agent_sdk: crate::providers::claude_agent_sdk::ClaudeAgentSdkConfig,
    /// Optional host-owned MCP endpoint provider.
    pub mcp_provider: Option<Arc<dyn crate::providers::claude_code::driver::McpEndpointProvider>>,
}

/// Builds one supported Claude subprocess route, returning `None` for other
/// provider kinds. Hosts must run their [`crate::host::model_factory::ModelBuildGate`] before calling this
/// function. The default Claude Code constructor probes the installed CLI and
/// resolves its credential through the existing provider; it does not mutate
/// the process environment. Use [`build_claude_subprocess_model_with`] to
/// inject a constructor in tests or hosts that resolve these values elsewhere.
#[cfg(feature = "claude-code")]
pub fn build_claude_subprocess_model(
    route: &tinyinference_providers::route::ProviderRoute,
    context: &SubprocessProviderContext,
) -> anyhow::Result<Option<Arc<dyn ChatModel<()>>>> {
    use crate::providers::claude_code::ClaudeCodeProvider;

    let documents = Arc::clone(&context.documents);
    build_claude_subprocess_model_with(route, context, |model, workspace_dir, action_dir, mcp| {
        // Keep the host's existing environment-based CLI discovery and auth
        // resolution. This constructor probes the installed CLI and resolves
        // its credential; hosts must run their build gate before this call.
        let mut provider = ClaudeCodeProvider::from_env(
            model,
            workspace_dir.to_path_buf(),
            action_dir.to_path_buf(),
            documents,
        )?;
        if let Some(mcp) = mcp {
            provider = provider.with_mcp_provider(mcp);
        }
        Ok(Arc::new(provider) as Arc<dyn ChatModel<()>>)
    })
}

/// Routes supported Claude subprocess providers through their existing
/// adapters. `build_claude_code` is called only for a non-empty Claude Code
/// model and receives the host's selected model, workspace, action directory,
/// and optional MCP endpoint. Injecting this constructor makes route assembly
/// testable without CLI discovery, credentials, environment access, or a
/// subprocess. The SDK route is built from the host-supplied SDK config.
#[cfg(feature = "claude-code")]
pub fn build_claude_subprocess_model_with<F>(
    route: &tinyinference_providers::route::ProviderRoute,
    context: &SubprocessProviderContext,
    build_claude_code: F,
) -> anyhow::Result<Option<Arc<dyn ChatModel<()>>>>
where
    F: FnOnce(
        &str,
        &std::path::Path,
        &std::path::Path,
        Option<Arc<dyn crate::providers::claude_code::driver::McpEndpointProvider>>,
    ) -> anyhow::Result<Arc<dyn ChatModel<()>>>,
{
    use crate::providers::claude_agent_sdk::ClaudeAgentSdkProvider;
    use tinyinference_providers::route::RouteKind;

    match route.kind() {
        RouteKind::ClaudeCode => {
            anyhow::ensure!(
                !route.model.trim().is_empty(),
                "Claude Code model must not be empty"
            );
            let model = build_claude_code(
                &route.model,
                &context.workspace_dir,
                &context.action_dir,
                context.mcp_provider.clone(),
            )?;
            Ok(Some(model))
        }
        RouteKind::ClaudeAgentSdk => Ok(Some(Arc::new(ClaudeAgentSdkProvider::for_model(
            context.claude_agent_sdk.clone(),
            route.model.clone(),
        )))),
        _ => Ok(None),
    }
}
