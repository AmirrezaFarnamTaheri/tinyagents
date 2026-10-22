//! Reusable agent-loop execution helpers.

use crate::context::RunContext;
use crate::error::Result;
use crate::events::HarnessRunStatus;
use crate::middleware::AgentRun;
use crate::runtime::AgentHarness;
use tinyinference_llm::message::Message;
use tinyinference_llm::tool::ToolCall;

/// The messages and tool names produced by executing one batch of calls.
#[derive(Clone, Debug)]
pub struct ToolBatchOutcome {
    /// Tool-result messages appended in call order.
    pub results: Vec<Message>,
    /// Calls that reached an executor, in execution order.
    pub executed_tools: Vec<String>,
}

/// Executes a batch with the same admission, middleware, and result-folding
/// pipeline used by the standard agent loop.
pub async fn execute_tool_batch<State: Send + Sync, Ctx: Send + Sync>(
    harness: &AgentHarness<State, Ctx>,
    state: &State,
    ctx: &mut RunContext<Ctx>,
    run: &mut AgentRun,
    status: &mut HarnessRunStatus,
    messages: &mut Vec<Message>,
    tool_calls: Vec<ToolCall>,
) -> Result<ToolBatchOutcome> {
    let messages_before = messages.len();
    let executed_before = run.executed_tools.len();
    harness
        .execute_tools_with_promotions(
            state,
            ctx,
            run,
            status,
            messages,
            tool_calls,
            &mut std::collections::BTreeSet::new(),
        )
        .await?;
    Ok(ToolBatchOutcome {
        results: messages[messages_before..].to_vec(),
        executed_tools: run.executed_tools[executed_before..].to_vec(),
    })
}
