//! Typed live-parent dispatch for packed tools.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tinytools::{Tool, ToolCallOptions, ToolResult};

use tinyagents_harness::{
    context::RunContext,
    tool::{BoundToolCall, ToolCallBinding, ToolDispatch, ToolExecutionContext},
};

use super::{PackSelection, PinnedPackCall};

/// Resolves a selected packed tool into the host's normal typed dispatcher.
///
/// The host can use [`PackSelection::registry`] and [`PackSelection::tool_name`]
/// to construct its usual adapter. Returning `None` keeps ordinary tools on
/// the canonical path; recursive tools should return the exact dispatch used
/// when they are directly registered.
pub type PackDispatchSelector<State, Ctx> =
    dyn Fn(PackSelection) -> Option<Arc<dyn ToolDispatch<State, Ctx>>> + Send + Sync;

/// A `use_skill` registration that can route packed calls through the live
/// typed parent while retaining the call-scoped target used for admission.
pub struct UseSkillDispatch<State: Send + Sync + 'static, Ctx: Send + Sync + 'static> {
    tool: Arc<dyn Tool>,
    selector: Arc<PackDispatchSelector<State, Ctx>>,
}

impl<State: Send + Sync + 'static, Ctx: Send + Sync + 'static> UseSkillDispatch<State, Ctx> {
    /// Creates a dispatch around the registered `use_skill` declaration.
    pub fn new(tool: Arc<dyn Tool>, selector: Arc<PackDispatchSelector<State, Ctx>>) -> Self {
        Self { tool, selector }
    }
}

#[async_trait]
impl<State: Send + Sync + 'static, Ctx: Send + Sync + 'static> ToolDispatch<State, Ctx>
    for UseSkillDispatch<State, Ctx>
{
    fn tool(&self) -> Arc<dyn Tool> {
        self.tool.clone()
    }

    fn bind_call(
        &self,
        arguments: &Value,
    ) -> anyhow::Result<Option<Arc<dyn ToolDispatch<State, Ctx>>>> {
        let Some(extension) = self
            .tool
            .host_extension()
            .and_then(|extension| extension.downcast_ref::<ToolCallBinding>())
        else {
            anyhow::bail!("use_skill tool is missing its pack registry extension");
        };
        let pinned = extension.bind(self.tool.clone(), arguments);
        let selection = pinned
            .as_any()
            .downcast_ref::<PinnedPackCall>()
            .and_then(PinnedPackCall::selection);
        let selected = if let Some(selection) = selection {
            if let Some(dispatch) = (self.selector)(selection.clone()) {
                Some(
                    dispatch
                        .bind_call(selection.arguments())?
                        .unwrap_or(dispatch),
                )
            } else {
                None
            }
        } else {
            None
        };
        Ok(Some(Arc::new(BoundUseSkillDispatch { pinned, selected })))
    }

    async fn execute(
        &self,
        _state: &State,
        call_id: tinyagents_harness::ids::CallId,
        arguments: Value,
        options: ToolCallOptions,
        parent: &RunContext<Ctx>,
    ) -> anyhow::Result<ToolResult> {
        let dispatch = self
            .bind_call(&arguments)?
            .ok_or_else(|| anyhow::anyhow!("use_skill binding produced no dispatch"))?;
        dispatch
            .execute(_state, call_id, arguments, options, parent)
            .await
    }
}

struct BoundUseSkillDispatch<State: Send + Sync + 'static, Ctx: Send + Sync + 'static> {
    pinned: Arc<dyn BoundToolCall>,
    selected: Option<Arc<dyn ToolDispatch<State, Ctx>>>,
}

#[async_trait]
impl<State: Send + Sync + 'static, Ctx: Send + Sync + 'static> BoundUseSkillDispatch<State, Ctx> {
    fn selection(&self) -> Option<PackSelection> {
        self.pinned
            .as_any()
            .downcast_ref::<PinnedPackCall>()
            .and_then(PinnedPackCall::selection)
    }
}

impl<State: Send + Sync + 'static, Ctx: Send + Sync + 'static> ToolDispatch<State, Ctx>
    for BoundUseSkillDispatch<State, Ctx>
{
    fn tool(&self) -> Arc<dyn Tool> {
        self.pinned.tool()
    }

    fn admission_target(&self) -> Option<(&dyn Tool, &Value)> {
        self.selected
            .as_ref()
            .and_then(|dispatch| dispatch.admission_target())
            .or_else(|| self.pinned.admission_target())
    }

    fn output_origin(&self) -> tinyagents_harness::host::ContentOrigin {
        self.selected
            .as_ref()
            .map_or(tinyagents_harness::host::ContentOrigin::Tool, |dispatch| {
                dispatch.output_origin()
            })
    }

    fn call_options(&self, arguments: &Value) -> ToolCallOptions {
        let inner_arguments = self
            .selection()
            .map(|selection| selection.arguments().clone())
            .unwrap_or_else(|| arguments.clone());
        self.selected.as_ref().map_or_else(
            || {
                if self.pinned.tool().supports_markdown() {
                    ToolCallOptions {
                        prefer_markdown: true,
                    }
                } else {
                    ToolCallOptions::default()
                }
            },
            |dispatch| dispatch.call_options(&inner_arguments),
        )
    }

    async fn execute(
        &self,
        state: &State,
        call_id: tinyagents_harness::ids::CallId,
        _arguments: Value,
        options: ToolCallOptions,
        parent: &RunContext<Ctx>,
    ) -> anyhow::Result<ToolResult> {
        if let Some(dispatch) = &self.selected {
            let arguments = self
                .pinned
                .selection()
                .map(|selection| selection.arguments().clone())
                .unwrap_or(Value::Null);
            return dispatch
                .execute(state, call_id, arguments, options, parent)
                .await;
        }

        let context = ToolExecutionContext::from_run_context(parent, call_id);
        self.pinned
            .tool()
            .execute_with_context(Value::Null, options, Some(&context))
            .await
    }
}
